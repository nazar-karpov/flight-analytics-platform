"""
DAG: stg_to_dds  (runs every 5 minutes)

Pure pandas + s3fs version -- no PySpark, no JVM startup.
Reads STG Parquet from MinIO, deduplicates, applies SCD2 for aircraft_dim,
appends to flight_states_fact. Fast enough for streaming flight data.
"""
from __future__ import annotations

import io
import os
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")

default_args = {"owner": "airflow", "retries": 1}

# Region bounding boxes (simple approximation)
REGION_BOUNDS = {
    "Europe":       {"lat_min": 35, "lat_max": 72, "lon_min": -25, "lon_max": 45},
    "Asia":         {"lat_min": -10, "lat_max": 55, "lon_min": 45, "lon_max": 180},
    "NorthAmerica": {"lat_min": 15, "lat_max": 72, "lon_min": -170, "lon_max": -50},
    "SouthAmerica": {"lat_min": -56, "lat_max": 15, "lon_min": -82, "lon_max": -34},
    "Africa":       {"lat_min": -35, "lat_max": 37, "lon_min": -18, "lon_max": 52},
    "Oceania":      {"lat_min": -50, "lat_max": 0, "lon_min": 110, "lon_max": 180},
}


def classify_region(lat, lon) -> str:
    if lat is None or lon is None:
        return "Other"
    for region, b in REGION_BOUNDS.items():
        if b["lat_min"] <= lat <= b["lat_max"] and b["lon_min"] <= lon <= b["lon_max"]:
            return region
    return "Other"


def extract_airline_code(callsign) -> str:
    """Extract ICAO airline code from callsign (first 3 alpha characters)."""
    if not callsign or not isinstance(callsign, str):
        return ""
    cs = callsign.strip()
    if len(cs) >= 3 and cs[:3].isalpha():
        return cs[:3].upper()
    return ""


def _s3fs():
    import s3fs
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )


def process_stg_to_dds(**context):
    import pandas as pd

    fs = _s3fs()

    # -- 1. Read all STG parquet files -------------------------------------------
    try:
        stg_files = fs.glob("stg/flight_states/**/*.parquet")
    except Exception as exc:
        print(f"Cannot list STG files: {exc}")
        return

    if not stg_files:
        print("No STG parquet files yet -- nothing to process.")
        return

    frames = []
    for f in stg_files:
        try:
            with fs.open(f, "rb") as fh:
                frames.append(pd.read_parquet(fh))
        except Exception as exc:
            print(f"Skipping {f}: {exc}")

    if not frames:
        print("All STG files unreadable.")
        return

    df = pd.concat(frames, ignore_index=True)
    print(f"STG rows loaded: {len(df)}")

    # -- 2. Deduplicate by (icao24, time_position) ------------------------------
    df["time_position"] = pd.to_numeric(df["time_position"], errors="coerce")
    df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"], utc=True, errors="coerce")
    df = (
        df.sort_values("ingestion_ts", ascending=False)
          .drop_duplicates(subset=["icao24", "time_position"])
    )
    print(f"After dedup: {len(df)} rows")

    # -- 3. Enrich: airline_code and region -------------------------------------
    df["airline_code"] = df["callsign"].apply(extract_airline_code)
    df["region"] = df.apply(
        lambda r: classify_region(r.get("latitude"), r.get("longitude")), axis=1
    )

    # -- 4. SCD2 for aircraft_dim -----------------------------------------------
    now_ts = pd.Timestamp.now(tz="UTC")
    far_future = pd.Timestamp("9999-12-31 23:59:59", tz="UTC")

    dim_path = "dds/aircraft_dim/aircraft_dim.parquet"
    try:
        with fs.open(dim_path, "rb") as fh:
            dim = pd.read_parquet(fh)
        dim["valid_from"] = pd.to_datetime(dim["valid_from"], utc=True)
        dim["valid_to"] = pd.to_datetime(dim["valid_to"], utc=True)
    except Exception:
        dim = pd.DataFrame(columns=[
            "icao24", "callsign", "origin_country", "airline_code",
            "valid_from", "valid_to", "is_current",
        ])

    # Latest attrs per aircraft from incoming batch
    latest_attrs = (
        df.sort_values("ingestion_ts", ascending=False)
          .drop_duplicates(subset=["icao24"])[["icao24", "callsign", "origin_country", "airline_code"]]
    )

    current = dim[dim["is_current"] == True] if len(dim) > 0 else dim.copy()
    historical = dim[dim["is_current"] == False] if len(dim) > 0 else dim.copy()

    # New aircraft not in dim
    existing_ids = set(current["icao24"]) if len(current) > 0 else set()
    new_aircraft = latest_attrs[~latest_attrs["icao24"].isin(existing_ids)].copy()
    new_aircraft["valid_from"] = now_ts
    new_aircraft["valid_to"] = far_future
    new_aircraft["is_current"] = True

    # Changed aircraft (callsign or origin_country changed)
    if len(current) > 0:
        merged = latest_attrs.merge(
            current[["icao24", "callsign", "origin_country"]],
            on="icao24", suffixes=("_new", "_old"),
        )
        changed_ids = merged[
            (merged["callsign_new"] != merged["callsign_old"]) |
            (merged["origin_country_new"] != merged["origin_country_old"])
        ]["icao24"].tolist()
    else:
        changed_ids = []

    if changed_ids:
        current.loc[current["icao24"].isin(changed_ids), "valid_to"] = now_ts
        current.loc[current["icao24"].isin(changed_ids), "is_current"] = False
        new_versions = latest_attrs[latest_attrs["icao24"].isin(changed_ids)].copy()
        new_versions["valid_from"] = now_ts
        new_versions["valid_to"] = far_future
        new_versions["is_current"] = True
    else:
        new_versions = pd.DataFrame(columns=dim.columns)

    full_dim = pd.concat([historical, current, new_aircraft, new_versions], ignore_index=True)
    full_dim = full_dim[["icao24", "callsign", "origin_country", "airline_code",
                          "valid_from", "valid_to", "is_current"]]

    buf = io.BytesIO()
    full_dim.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(dim_path, "wb") as fh:
        fh.write(buf.read())
    print(f"SCD2 dim written: {len(full_dim)} records")

    # -- 5. Append to flight_states_fact ----------------------------------------
    fact_cols = [
        "icao24", "callsign", "time_position", "longitude", "latitude",
        "baro_altitude", "velocity", "true_track", "vertical_rate",
        "on_ground", "airline_code", "region", "ingestion_ts",
    ]
    fact = df[[c for c in fact_cols if c in df.columns]].copy()

    fact_path = "dds/flight_states_fact/flight_states_fact.parquet"
    try:
        with fs.open(fact_path, "rb") as fh:
            existing_fact = pd.read_parquet(fh)
        existing_fact["time_position"] = pd.to_numeric(existing_fact["time_position"], errors="coerce")
        # Deduplicate against existing
        existing_keys = set(zip(existing_fact["icao24"], existing_fact["time_position"]))
        fact = fact[~fact.apply(lambda r: (r["icao24"], r["time_position"]) in existing_keys, axis=1)]
        fact = pd.concat([existing_fact, fact], ignore_index=True)
    except Exception:
        pass  # first run

    buf = io.BytesIO()
    fact.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(fact_path, "wb") as fh:
        fh.write(buf.read())
    print(f"Fact written: {len(fact)} total rows")


with DAG(
    dag_id="stg_to_dds",
    description="STG -> DDS: SCD2 aircraft_dim + flight_states_fact (pandas)",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dds"],
) as dag:
    PythonOperator(
        task_id="stg_to_dds_task",
        python_callable=process_stg_to_dds,
    )

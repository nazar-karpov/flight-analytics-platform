"""
DAG: dds_to_ddm  (runs every 5 minutes)

Pure pandas version -- no PySpark, no JVM.
Reads DDS Parquet from MinIO, computes 5 flight marts, writes to ClickHouse.
"""
from __future__ import annotations

import csv
import io
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
CLICKHOUSE_HOST  = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT  = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB    = os.environ.get("CLICKHOUSE_DB", "flights")

default_args = {"owner": "airflow", "retries": 1}

# Seed data paths (mounted via docker volume)
SEED_DIR = "/opt/airflow/seed"


def _load_airports():
    """Load airports seed CSV."""
    airports = []
    path = os.path.join(SEED_DIR, "airports.csv")
    if not os.path.exists(path):
        print(f"Warning: {path} not found")
        return airports
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["latitude"] = float(row["latitude"])
            row["longitude"] = float(row["longitude"])
            airports.append(row)
    return airports


def _haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _s3fs():
    import s3fs
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )


def _ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        username="default",
        password="",
    )


def _insert(ch, table: str, df):
    if df is None or df.empty:
        print(f"  [skip] {table}: empty")
        return
    ch.insert_df(f"{CLICKHOUSE_DB}.{table}", df)
    print(f"  [ok] {table}: {len(df)} rows")


def compute_marts(**context):
    import pandas as pd
    import numpy as np

    fs = _s3fs()
    ch = _ch()

    # -- Load DDS ---------------------------------------------------------------
    try:
        with fs.open("dds/flight_states_fact/flight_states_fact.parquet", "rb") as fh:
            fact = pd.read_parquet(fh)
        with fs.open("dds/aircraft_dim/aircraft_dim.parquet", "rb") as fh:
            dim = pd.read_parquet(fh)
    except Exception as exc:
        print(f"DDS not ready yet: {exc}")
        return

    if fact.empty:
        print("Fact table is empty -- skipping.")
        return

    fact["time_position"] = pd.to_numeric(fact["time_position"], errors="coerce")
    fact["ingestion_ts"] = pd.to_datetime(fact["ingestion_ts"], utc=True, errors="coerce")
    fact["ts"] = pd.to_datetime(fact["time_position"], unit="s", utc=True, errors="coerce")
    print(f"Fact rows: {len(fact)}")

    now = pd.Timestamp.now(tz="UTC")
    airports = _load_airports()

    # ===== Mart 1: mart_flights_current ========================================
    # Last position per aircraft, seen within last 15 minutes
    cutoff = now - pd.Timedelta(minutes=15)
    recent = fact[fact["ingestion_ts"] >= cutoff].copy()
    if not recent.empty:
        current = (
            recent.sort_values("time_position", ascending=False)
            .drop_duplicates(subset=["icao24"])
        )
        current["last_seen"] = current["ts"].fillna(now)
        mart_current = current[[
            "icao24", "callsign", "airline_code", "longitude", "latitude",
            "baro_altitude", "velocity", "true_track", "region",
        ]].copy()
        # Get origin_country from dim
        dim_current = dim[dim["is_current"] == True][["icao24", "origin_country"]]
        mart_current = mart_current.merge(dim_current, on="icao24", how="left")
        mart_current["last_seen"] = current["last_seen"].values
        mart_current["updated_at"] = pd.Timestamp.now()
        # Fill NaN strings and convert types for ClickHouse
        for col in ["icao24", "callsign", "airline_code", "origin_country", "region"]:
            if col in mart_current.columns:
                mart_current[col] = mart_current[col].fillna("").astype(str)
        mart_current["last_seen"] = pd.to_datetime(mart_current["last_seen"], utc=True).dt.tz_localize(None)
        mart_current["updated_at"] = mart_current["updated_at"].tz_localize(None) if mart_current["updated_at"].dt.tz else mart_current["updated_at"]
        _insert(ch, "mart_flights_current", mart_current)
    else:
        print("  [skip] mart_flights_current: no recent data")

    # ===== Mart 2: mart_airport_traffic_hourly =================================
    if airports and not fact.empty:
        fact_with_coords = fact.dropna(subset=["latitude", "longitude"]).copy()
        fact_with_coords["hour"] = fact_with_coords["ts"].dt.floor("h")

        rows = []
        for apt in airports:
            apt_lat, apt_lon = apt["latitude"], apt["longitude"]
            # Vectorized approximate distance filter (rough bounding box ~50km)
            delta = 0.5  # ~50km at mid latitudes
            nearby = fact_with_coords[
                (fact_with_coords["latitude"].between(apt_lat - delta, apt_lat + delta)) &
                (fact_with_coords["longitude"].between(apt_lon - delta, apt_lon + delta))
            ]
            if nearby.empty:
                continue
            # Precise haversine filter
            nearby = nearby[nearby.apply(
                lambda r: _haversine_km(r["latitude"], r["longitude"], apt_lat, apt_lon) <= 50,
                axis=1,
            )]
            if nearby.empty:
                continue
            hourly = nearby.groupby("hour")["icao24"].nunique().reset_index()
            hourly.columns = ["hour", "unique_aircraft"]
            hourly["airport_iata"] = apt["iata"]
            hourly["airport_name"] = apt["name"]
            hourly["updated_at"] = pd.Timestamp.now()
            rows.append(hourly)

        if rows:
            mart_airport = pd.concat(rows, ignore_index=True)
            mart_airport = mart_airport[["airport_iata", "airport_name", "hour",
                                          "unique_aircraft", "updated_at"]]
            mart_airport["hour"] = pd.to_datetime(mart_airport["hour"]).dt.tz_localize(None)
            mart_airport["updated_at"] = pd.Timestamp.now()
            mart_airport["unique_aircraft"] = mart_airport["unique_aircraft"].astype("uint32")
            _insert(ch, "mart_airport_traffic_hourly", mart_airport)

    # ===== Mart 3: mart_airline_stats_daily ====================================
    fact_today = fact[fact["ts"] >= now.normalize()].copy()
    if not fact_today.empty:
        grouped = fact_today.groupby("airline_code").agg(
            flight_count=("icao24", "nunique"),
            avg_altitude=("baro_altitude", "mean"),
            avg_speed=("velocity", "mean"),
            countries=("region", "nunique"),
        ).reset_index()
        grouped = grouped[grouped["airline_code"] != ""]
        grouped["date"] = now.date()
        grouped["updated_at"] = pd.Timestamp.now()
        grouped["avg_altitude"] = grouped["avg_altitude"].fillna(0).round(1)
        grouped["avg_speed"] = grouped["avg_speed"].fillna(0).round(1)
        grouped = grouped[["airline_code", "date", "flight_count", "avg_altitude",
                            "avg_speed", "countries", "updated_at"]]
        grouped["flight_count"] = grouped["flight_count"].astype("uint32")
        grouped["countries"] = grouped["countries"].astype("uint16")
        _insert(ch, "mart_airline_stats_daily", grouped)

    # ===== Mart 4: mart_region_traffic_current =================================
    if not recent.empty:
        latest_per_aircraft = (
            recent.sort_values("time_position", ascending=False)
            .drop_duplicates(subset=["icao24"])
        )
        region_counts = (
            latest_per_aircraft.groupby("region")["icao24"]
            .nunique()
            .reset_index()
            .rename(columns={"icao24": "aircraft_count"})
        )
        region_counts["snapshot_time"] = pd.Timestamp.now()
        region_counts["updated_at"] = pd.Timestamp.now()
        region_counts["aircraft_count"] = region_counts["aircraft_count"].astype("uint32")
        _insert(ch, "mart_region_traffic_current", region_counts)

    # ===== Mart 5: mart_flight_anomalies =======================================
    anomalies = []

    # a) low_altitude: baro_altitude < 500m, not on_ground, far from airport
    if not recent.empty:
        low_alt = recent[
            (recent["baro_altitude"].notna()) &
            (recent["baro_altitude"] < 500) &
            (recent["baro_altitude"] > 0) &
            (recent["on_ground"] == False) &
            (recent["latitude"].notna()) &
            (recent["longitude"].notna())
        ].copy()

        for _, row in low_alt.iterrows():
            near_airport = False
            for apt in airports:
                if _haversine_km(row["latitude"], row["longitude"],
                                 apt["latitude"], apt["longitude"]) < 50:
                    near_airport = True
                    break
            if not near_airport:
                anomalies.append({
                    "icao24": row["icao24"],
                    "callsign": row.get("callsign", ""),
                    "anomaly_type": "low_altitude",
                    "detected_at": pd.Timestamp.now(),
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "details": json.dumps({"baro_altitude": row["baro_altitude"]}),
                })

    # b) unusual_speed: velocity > 95th percentile
    if not recent.empty and recent["velocity"].notna().sum() > 10:
        p95 = recent["velocity"].quantile(0.95)
        fast = recent[recent["velocity"] > p95]
        for _, row in fast.head(50).iterrows():
            anomalies.append({
                "icao24": row["icao24"],
                "callsign": row.get("callsign", ""),
                "anomaly_type": "unusual_speed",
                "detected_at": pd.Timestamp.now(),
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "details": json.dumps({"velocity": row["velocity"], "p95_threshold": round(p95, 1)}),
            })

    if anomalies:
        anom_df = pd.DataFrame(anomalies)
        anom_df["updated_at"] = pd.Timestamp.now()
        for col in ["icao24", "callsign", "anomaly_type", "details"]:
            anom_df[col] = anom_df[col].fillna("").astype(str)
        anom_df["detected_at"] = pd.to_datetime(anom_df["detected_at"]).dt.tz_localize(None)
        _insert(ch, "mart_flight_anomalies", anom_df)
    else:
        print("  [skip] mart_flight_anomalies: none detected")

    print("All marts done.")


with DAG(
    dag_id="dds_to_ddm",
    description="DDS -> DDM: compute 5 flight marts (pandas)",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ddm", "clickhouse"],
) as dag:

    wait_for_dds = ExternalTaskSensor(
        task_id="wait_for_stg_to_dds",
        external_dag_id="stg_to_dds",
        external_task_id="stg_to_dds_task",
        timeout=3600,
        mode="reschedule",
        poke_interval=30,
    )

    compute = PythonOperator(
        task_id="compute_marts",
        python_callable=compute_marts,
    )

    wait_for_dds >> compute

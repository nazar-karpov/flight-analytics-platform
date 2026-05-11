"""
DAG: initial_load  (schedule='@once', trigger manually)

Takes a single snapshot from OpenSky /states/all and writes it
directly to STG layer (s3://stg/flight_states/) in the same Parquet
schema as the streaming consumer, so the regular stg_to_dds DAG can process it.

Unlike CoinGecko, OpenSky has no free historical API,
so this DAG just bootstraps with one current snapshot.
"""
from __future__ import annotations

import io
import json
import os
import time
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
OPENSKY_URL      = "https://opensky-network.org/api/states/all"
OPENSKY_USER     = os.environ.get("OPENSKY_USER", "")
OPENSKY_PASS     = os.environ.get("OPENSKY_PASS", "")

FIELD_NAMES = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]


def _s3fs():
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )


def snapshot_load(**context):
    auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER and OPENSKY_PASS else None

    log.info("Fetching OpenSky snapshot...")
    backoff = 5
    states = None
    for attempt in range(5):
        try:
            resp = requests.get(OPENSKY_URL, auth=auth, timeout=30)
            if resp.status_code in (429, 503):
                wait = int(resp.headers.get("Retry-After", backoff))
                log.warning("Rate limited (%d). Sleeping %ds...", resp.status_code, wait)
                time.sleep(wait)
                backoff = min(backoff * 2, 300)
                continue
            resp.raise_for_status()
            states = resp.json().get("states", [])
            break
        except requests.RequestException as exc:
            log.error("Attempt %d failed: %s", attempt + 1, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)

    if not states:
        log.warning("No states received from OpenSky.")
        return

    log.info("Received %d state vectors.", len(states))

    records = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for sv in states:
        row = {}
        for i, name in enumerate(FIELD_NAMES):
            if i < len(sv):
                row[name] = sv[i]
            else:
                row[name] = None
        if row.get("callsign"):
            row["callsign"] = row["callsign"].strip()
        row["on_ground"] = bool(row.get("on_ground", False))
        row.pop("sensors", None)
        row["ingestion_ts"] = now_iso
        records.append(row)

    df = pd.DataFrame(records)
    now = datetime.now(timezone.utc)
    path = (
        f"stg/flight_states/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"initial_snapshot.parquet"
    )

    fs = _s3fs()
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(path, "wb") as fh:
        fh.write(buf.read())
    log.info("Wrote %d rows to %s", len(df), path)


with DAG(
    dag_id="initial_load",
    description="One-time OpenSky snapshot -> STG",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["backfill", "stg"],
) as dag:
    PythonOperator(
        task_id="snapshot_load",
        python_callable=snapshot_load,
        execution_timeout=timedelta(minutes=10),
    )

"""
Simple Kafka -> MinIO consumer (replaces Spark Streaming).
Reads JSON messages from Kafka, batches them, writes Parquet to MinIO/STG.
Runs as a plain Python process -- no JVM, no Spark.
"""
import io
import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import s3fs
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "raw_flight_states")
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET    = os.environ.get("MINIO_SECRET_KEY", "password123")

BATCH_SIZE    = 500   # write to MinIO every N messages (flights produce many records)
FLUSH_EVERY_S = 30    # or every 30 seconds, whichever comes first


def get_s3():
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS, secret=MINIO_SECRET,
        endpoint_url=MINIO_ENDPOINT, use_ssl=False,
    )


def write_batch(fs, records: list):
    if not records:
        return
    df = pd.DataFrame(records)

    now = datetime.now(timezone.utc)
    path = (
        f"stg/flight_states/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{int(now.timestamp())}.parquet"
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(path, "wb") as fh:
        fh.write(buf.read())
    log.info("Wrote %d rows -> %s", len(df), path)


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "stg-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to %s", KAFKA_TOPIC)

    fs = get_s3()
    batch = []
    last_flush = time.time()

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("Kafka error: %s", msg.error())
        else:
            try:
                record = json.loads(msg.value().decode("utf-8"))
                batch.append(record)
            except Exception as exc:
                log.warning("Bad message: %s", exc)

        # Flush if batch full OR timeout reached
        if len(batch) >= BATCH_SIZE or (batch and time.time() - last_flush >= FLUSH_EVERY_S):
            try:
                write_batch(fs, batch)
            except Exception as exc:
                log.error("Failed to write batch: %s", exc)
            batch = []
            last_flush = time.time()


if __name__ == "__main__":
    main()

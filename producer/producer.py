"""
Kafka producer: polls OpenSky Network /states/all every 10s,
sends each aircraft state-vector as a JSON message to topic raw_flight_states.
"""
import json
import os
import time
import logging
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_flight_states")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "10"))
OPENSKY_URL = "https://opensky-network.org/api/states/all"
OPENSKY_USER = os.environ.get("OPENSKY_USER", "")
OPENSKY_PASS = os.environ.get("OPENSKY_PASS", "")

# Exponential backoff settings
MAX_BACKOFF = 300  # 5 minutes max


def delivery_callback(err, msg):
    if err:
        log.error("Message delivery failed: %s", err)
    else:
        log.debug("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())


def fetch_states() -> list[dict]:
    """Fetch all aircraft state vectors from OpenSky. Returns [] on transient errors."""
    auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER and OPENSKY_PASS else None
    backoff = 5
    for attempt in range(5):
        try:
            response = requests.get(OPENSKY_URL, auth=auth, timeout=30)
            if response.status_code in (429, 503):
                retry_after = int(response.headers.get("Retry-After", backoff))
                log.warning("Rate limited (%d). Sleeping %ds (attempt %d)...",
                            response.status_code, retry_after, attempt + 1)
                time.sleep(retry_after)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            response.raise_for_status()
            data = response.json()
            return data.get("states", []) or []
        except requests.RequestException as exc:
            log.error("OpenSky request failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
    return []


# OpenSky state vector field indices
# See: https://openskynetwork.github.io/opensky-api/rest.html
FIELD_NAMES = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]


def state_to_message(state: list) -> dict:
    """Convert an OpenSky state vector array to our schema dict."""
    msg = {}
    for i, name in enumerate(FIELD_NAMES):
        if i < len(state):
            msg[name] = state[i]
        else:
            msg[name] = None

    # Clean callsign (strip whitespace)
    if msg.get("callsign"):
        msg["callsign"] = msg["callsign"].strip()

    # Convert on_ground to bool
    msg["on_ground"] = bool(msg.get("on_ground", False))

    # Drop sensors array (not needed downstream)
    msg.pop("sensors", None)

    msg["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
    return msg


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 50,
        "batch.num.messages": 500,
    })
    log.info("Producer started. Kafka=%s  Topic=%s  Interval=%ds  Auth=%s",
             KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, POLL_INTERVAL,
             "yes" if OPENSKY_USER else "anonymous")

    while True:
        states = fetch_states()
        if states:
            count = 0
            for state in states:
                msg = state_to_message(state)
                icao24 = msg.get("icao24", "unknown")
                producer.produce(
                    KAFKA_TOPIC,
                    key=icao24.encode(),
                    value=json.dumps(msg).encode(),
                    callback=delivery_callback,
                )
                count += 1
                # Flush periodically to avoid buffer overflow
                if count % 1000 == 0:
                    producer.flush()
            producer.flush()
            log.info("Published %d state vectors to %s", count, KAFKA_TOPIC)
        else:
            log.warning("No data fetched; skipping this cycle")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()

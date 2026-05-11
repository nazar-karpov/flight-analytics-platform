-- ClickHouse initialization script
-- Creates the flights database and all data mart tables

CREATE DATABASE IF NOT EXISTS flights;

-- mart_flights_current: last known position of each aircraft in the air
CREATE TABLE IF NOT EXISTS flights.mart_flights_current
(
    icao24          String,
    callsign        String,
    airline_code    String,
    longitude       Nullable(Float64),
    latitude        Nullable(Float64),
    baro_altitude   Nullable(Float64),
    velocity        Nullable(Float64),
    true_track      Nullable(Float64),
    origin_country  String,
    region          String,
    last_seen       DateTime,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY icao24
SETTINGS index_granularity = 8192;

-- mart_airport_traffic_hourly: unique aircraft near top airports per hour
CREATE TABLE IF NOT EXISTS flights.mart_airport_traffic_hourly
(
    airport_iata    String,
    airport_name    String,
    hour            DateTime,
    unique_aircraft UInt32,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (airport_iata, hour)
SETTINGS index_granularity = 8192;

-- mart_airline_stats_daily: daily stats per airline code
CREATE TABLE IF NOT EXISTS flights.mart_airline_stats_daily
(
    airline_code    String,
    date            Date,
    flight_count    UInt32,
    avg_altitude    Float64,
    avg_speed       Float64,
    countries       UInt16,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (airline_code, date)
SETTINGS index_granularity = 8192;

-- mart_region_traffic_current: aircraft count by region right now
CREATE TABLE IF NOT EXISTS flights.mart_region_traffic_current
(
    region          String,
    aircraft_count  UInt32,
    snapshot_time   DateTime,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY region
SETTINGS index_granularity = 8192;

-- mart_flight_anomalies: detected anomalies
CREATE TABLE IF NOT EXISTS flights.mart_flight_anomalies
(
    icao24          String,
    callsign        String,
    anomaly_type    String,
    detected_at     DateTime,
    latitude        Nullable(Float64),
    longitude       Nullable(Float64),
    details         String,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(detected_at)
ORDER BY (anomaly_type, detected_at, icao24)
SETTINGS index_granularity = 8192;

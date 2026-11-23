-- Created automatically the first time the Postgres container starts.

CREATE TABLE IF NOT EXISTS hourly_trip_stats (
    pickup_hour      TIMESTAMP PRIMARY KEY,
    trips            BIGINT           NOT NULL,
    avg_distance_mi  DOUBLE PRECISION,
    avg_fare_usd     DOUBLE PRECISION,
    avg_duration_min DOUBLE PRECISION,
    total_revenue    DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_hourly_trip_stats_hour
    ON hourly_trip_stats (pickup_hour);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            SERIAL PRIMARY KEY,
    run_ts            TIMESTAMP DEFAULT now(),
    rows_in           BIGINT,
    rows_rejected     BIGINT,
    rows_out          BIGINT,
    hourly_rows       BIGINT,
    runtime_seconds   DOUBLE PRECISION
);

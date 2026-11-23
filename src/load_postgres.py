"""Stage 3 - Database integration: publish hourly aggregates to PostgreSQL."""
import logging
import time

import pandas as pd
from sqlalchemy import create_engine, text

from . import config

log = logging.getLogger("load")


def wait_for_db(engine, attempts: int = 20, delay: int = 3) -> None:
    for i in range(1, attempts + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:
            log.warning("Database not ready (%d/%d): %s", i, attempts, exc.__class__.__name__)
            time.sleep(delay)
    raise RuntimeError("PostgreSQL did not become available")


def load_to_postgres(report: dict, runtime_seconds: float) -> int:
    engine = create_engine(config.postgres_url())
    wait_for_db(engine)

    hourly = pd.read_parquet(config.PROCESSED_DIR / "hourly_trip_stats.parquet")
    hourly["pickup_hour"] = pd.to_datetime(hourly["pickup_hour"])

    # Single transaction: either everything is replaced or nothing changes.
    # TRUNCATE + insert makes reruns idempotent (no duplicate rows).
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE hourly_trip_stats"))
        hourly.to_sql("hourly_trip_stats", conn, if_exists="append", index=False, method="multi", chunksize=500)
        conn.execute(
            text(
                "INSERT INTO pipeline_runs (rows_in, rows_rejected, rows_out, hourly_rows, runtime_seconds) "
                "VALUES (:rows_in, :rows_rejected, :rows_out, :hourly_rows, :runtime)"
            ),
            {
                "rows_in": report["rows_in"],
                "rows_rejected": report["rows_rejected"],
                "rows_out": report["rows_out"],
                "hourly_rows": report["hourly_rows"],
                "runtime": runtime_seconds,
            },
        )

    with engine.connect() as conn:
        loaded = conn.execute(text("SELECT count(*) FROM hourly_trip_stats")).scalar()
    if loaded != len(hourly):
        raise RuntimeError(f"Row count mismatch: expected {len(hourly)}, found {loaded}")
    log.info("Loaded %d hourly rows into PostgreSQL (verified)", loaded)
    return int(loaded)

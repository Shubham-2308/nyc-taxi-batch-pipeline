"""Unit tests for the cleaning and aggregation logic (run with: pytest -q)."""
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.process import aggregate_hourly, quality_counts, tag_rejections

START, END = "2024-01-01", "2024-04-01"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_df(spark, rows):
    """Build timestamps from strings so they are parsed in the Spark session time zone (UTC),
    exactly like the pipeline treats the TLC wall-clock times. This keeps the tests independent
    of the time zone of the machine running them."""
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "total_amount"]
    as_text = [(a.isoformat(sep=" "), b.isoformat(sep=" "), d, f, t) for a, b, d, f, t in rows]
    df = spark.createDataFrame(as_text, cols)
    return (
        df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    )


def ts(day, hour, minute=0):
    return datetime(2024, 1, day, hour, minute)


def test_each_rule_rejects_with_expected_reason(spark):
    rows = [
        (ts(1, 8), ts(1, 8, 20), 3.0, 15.0, 20.0),            # valid
        (ts(1, 8), ts(1, 7, 50), 3.0, 15.0, 20.0),            # dropoff before pickup
        (ts(1, 8), ts(1, 8, 20), 0.0, 15.0, 20.0),            # zero distance
        (ts(1, 8), ts(1, 8, 20), 3.0, -5.0, 20.0),            # negative fare
        (ts(1, 8), ts(1, 8, 0), 3.0, 15.0, 20.0),             # zero duration -> dropoff not after pickup
        (datetime(2023, 12, 31, 23), datetime(2024, 1, 1, 0, 10), 3.0, 15.0, 20.0),  # pickup outside period
        (ts(1, 8), ts(2, 20), 3.0, 15.0, 20.0),               # > 6 hours
        (ts(1, 8), ts(1, 8, 20), None, 15.0, 20.0),           # missing distance
    ]
    tagged = tag_rejections(make_df(spark, rows), START, END)
    report = quality_counts(tagged)

    assert report["rows_in"] == 8
    assert report["rows_out"] == 1
    assert report["rows_rejected"] == 7
    assert report["rejected_by_reason"] == {
        "dropoff_not_after_pickup": 2,
        "invalid_distance": 1,
        "invalid_fare": 1,
        "pickup_outside_period": 1,
        "implausible_duration": 1,
        "missing_value": 1,
    }


def test_hourly_aggregation(spark):
    rows = [
        (ts(1, 8, 5), ts(1, 8, 25), 2.0, 10.0, 12.0),
        (ts(1, 8, 40), ts(1, 9, 0), 4.0, 20.0, 24.0),
        (ts(1, 9, 10), ts(1, 9, 20), 1.0, 8.0, 10.0),
    ]
    tagged = tag_rejections(make_df(spark, rows), START, END)
    valid = tagged.filter("reject_reason IS NULL")
    result = {r["pickup_hour"]: r for r in aggregate_hourly(valid).collect()}

    assert set(result) == {"2024-01-01 08:00:00", "2024-01-01 09:00:00"}
    eight = result["2024-01-01 08:00:00"]
    assert eight["trips"] == 2
    assert eight["avg_distance_mi"] == 3.0
    assert eight["total_revenue"] == 36.0
    assert result["2024-01-01 09:00:00"]["trips"] == 1

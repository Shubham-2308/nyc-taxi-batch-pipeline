"""Stage 2 - Processing with PySpark: validate, clean and aggregate trip records."""
import json
import logging
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config

log = logging.getLogger("process")

# Columns we keep and the type each one is cast to. Casting per file avoids schema
# conflicts, because the TLC changes column types between months (e.g. int vs double).
COLUMNS = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "fare_amount": "double",
    "total_amount": "double",
    "PULocationID": "int",
    "DOLocationID": "int",
}

MAX_DISTANCE_MILES = 200
MIN_DURATION_MIN = 1
MAX_DURATION_MIN = 360


def get_spark(app_name: str = "nyc-taxi-batch") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_files(spark: SparkSession, files) -> DataFrame:
    frames = []
    for path in files:
        df = spark.read.parquet(str(path))
        frames.append(df.select([F.col(c).cast(t).alias(c) for c, t in COLUMNS.items()]))
        log.info("Loaded %s", Path(path).name)
    return reduce(DataFrame.unionByName, frames)


def tag_rejections(df: DataFrame, start: str, end: str) -> DataFrame:
    """Add duration_min and reject_reason. reject_reason is NULL for valid rows."""
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    duration = (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60.0
    df = df.withColumn("duration_min", duration)

    distance = F.col("trip_distance")
    fare = F.col("fare_amount")
    total = F.col("total_amount")
    start_ts = F.to_timestamp(F.lit(start))
    end_ts = F.to_timestamp(F.lit(end))

    reason = (
        F.when(pickup.isNull() | dropoff.isNull(), "missing_timestamp")
        .when(distance.isNull() | fare.isNull() | total.isNull(), "missing_value")
        .when(dropoff <= pickup, "dropoff_not_after_pickup")
        .when((pickup < start_ts) | (pickup >= end_ts), "pickup_outside_period")
        .when(
            (F.col("duration_min") < MIN_DURATION_MIN) | (F.col("duration_min") > MAX_DURATION_MIN),
            "implausible_duration",
        )
        .when((distance <= 0) | (distance > MAX_DISTANCE_MILES), "invalid_distance")
        .when(fare <= 0, "invalid_fare")
        .when(total <= 0, "invalid_total")
        .otherwise(F.lit(None).cast("string"))
    )
    return df.withColumn("reject_reason", reason)


def aggregate_hourly(valid: DataFrame) -> DataFrame:
    """Hourly demand statistics. pickup_hour is a string so it travels safely into PostgreSQL."""
    hour = F.date_format(F.date_trunc("hour", F.col("tpep_pickup_datetime")), "yyyy-MM-dd HH:mm:ss")
    return (
        valid.groupBy(hour.alias("pickup_hour"))
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("trip_distance"), 3).alias("avg_distance_mi"),
            F.round(F.avg("fare_amount"), 3).alias("avg_fare_usd"),
            F.round(F.avg("duration_min"), 3).alias("avg_duration_min"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("pickup_hour")
    )


def quality_counts(tagged: DataFrame) -> dict:
    """One pass over the data: counts per rejection reason (None key = valid rows)."""
    rows = tagged.groupBy("reject_reason").count().collect()
    by_reason = {r["reject_reason"]: r["count"] for r in rows}
    rows_out = by_reason.pop(None, 0)
    rows_rejected = sum(by_reason.values())
    return {
        "rows_in": rows_out + rows_rejected,
        "rows_rejected": rows_rejected,
        "rows_out": rows_out,
        "rejected_by_reason": dict(sorted(by_reason.items(), key=lambda kv: -kv[1])),
    }


def run_processing(files) -> dict:
    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    config.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    start, end = config.period_bounds()

    spark = get_spark()
    try:
        raw = load_files(spark, files)
        tagged = tag_rejections(raw, start, end)

        log.info("Counting valid and rejected rows ...")
        report = quality_counts(tagged)
        log.info("Rows in=%d rejected=%d out=%d", report["rows_in"], report["rows_rejected"], report["rows_out"])

        valid = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
        clean_dir = config.PROCESSED_DIR / "clean_trips"
        log.info("Writing cleaned trips to %s", clean_dir)
        valid.write.mode("overwrite").parquet(str(clean_dir))

        log.info("Aggregating hourly statistics ...")
        hourly = aggregate_hourly(spark.read.parquet(str(clean_dir))).toPandas()
        hourly.to_parquet(config.PROCESSED_DIR / "hourly_trip_stats.parquet", index=False)
        hourly.to_csv(config.PROCESSED_DIR / "hourly_trip_stats.csv", index=False)
        report["hourly_rows"] = int(len(hourly))
        report["period"] = {"start": start, "end_exclusive": end}

        with open(config.REPORT_DIR / "quality_report.json", "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        return report
    finally:
        spark.stop()

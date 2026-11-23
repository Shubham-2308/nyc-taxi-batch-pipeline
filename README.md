# NYC Taxi Trip Data: Batch-Processing Pipeline (Docker, PySpark, PostgreSQL)

A reproducible batch data pipeline built for the IU Data Engineering portfolio. It processes
NYC Yellow Taxi trip records (January to March 2024, well over one million trips) and publishes
clean, aggregated data to PostgreSQL for downstream analysis such as demand forecasting.

## Architecture

```
NYC TLC Parquet files (3 months)
        |  1. ingest.py      download with retries, size check, skip if present
        v
   data/raw/*.parquet
        |  2. process.py     PySpark: cast schema, validate, tag rejections,
        v                    write clean Parquet, hourly aggregation, quality report
   data/processed/  +  data/reports/quality_report.json
        |  3. load_postgres.py   transactional TRUNCATE + insert, row-count check
        v
   PostgreSQL (hourly_trip_stats, pipeline_runs)

Docker Compose runs PostgreSQL and the pipeline container.
Windows Task Scheduler starts the run automatically (scripts/run_pipeline.bat).
```

| Stage | Tool | Why |
|---|---|---|
| Ingestion | Python + requests | Simple, testable, retry logic |
| Processing | PySpark 3.5 | Scales from a laptop to a cluster with the same code |
| Storage | Parquet | Columnar, compressed, typed |
| Database | PostgreSQL 16 | Reliable SQL target for analysts |
| Runtime | Docker Compose | Same environment on every machine |
| Scheduling | Windows Task Scheduler | Fits the local Windows setup |

## Quick start

Requirements: Docker Desktop (give it at least 6 GB RAM) and Git.

```bash
git clone <your-repo-url>
cd nyc-taxi-batch-pipeline
copy .env.example .env          # Windows (use cp on Mac/Linux), then edit the password
docker compose up -d postgres   # starts PostgreSQL and creates the tables
docker compose run --rm pipeline
```

The first run downloads about 150 MB of data and takes several minutes. When it ends you will see
`Pipeline finished`. Check the results:

```bash
docker compose exec postgres psql -U taxi_user -d taxi_db -c "SELECT count(*) FROM hourly_trip_stats;"
docker compose exec postgres psql -U taxi_user -d taxi_db -c "SELECT * FROM pipeline_runs;"
docker compose exec postgres psql -U taxi_user -d taxi_db -c "SELECT * FROM hourly_trip_stats ORDER BY trips DESC LIMIT 5;"
```

Other outputs: `data/reports/quality_report.json` (rows in/rejected/out and reasons),
`data/processed/hourly_trip_stats.csv`, `logs/pipeline.log`.

## Run the tests

```bash
docker compose run --rm pipeline python -m pytest -q tests
```

## Scheduling on Windows

1. Edit the folder path in `scripts/run_pipeline.bat`.
2. Open Task Scheduler, Create Basic Task, choose a schedule (for example monthly), action
   "Start a program", program = the full path of `run_pipeline.bat`.
3. Docker Desktop must be running when the task starts. Output goes to `logs/scheduler.log`.

## Data quality rules

A trip is rejected, with a recorded reason, when it has: missing timestamps or values,
dropoff not after pickup, pickup outside the processed period, duration under 1 or over 360
minutes, distance of 0 or above 200 miles, or a fare or total of 0 or less. Every run reports
how many rows were rejected per reason.

## Quality considerations

- **Reliability:** download retries and size validation; per-file schema casting (the TLC changes
  column types between months); transactional database load; row-count verification; non-zero exit on failure.
- **Reproducibility:** everything runs in Docker with pinned versions; configuration in `.env`;
  idempotent loads (rerunning never duplicates rows).
- **Security:** credentials only in `.env` (git-ignored); no credentials in code or compose file.
  For a real deployment use a least-privilege database user and a secrets manager.
- **Data protection and governance:** the TLC records contain no names or card numbers. The dataset is
  public; source and terms of use are listed below. Each run is logged in `pipeline_runs` for lineage.
- **Scalability:** Spark runs in local mode here. The same job can run on a cluster, and the
  data can move to object storage with partitioning by month.
- **Maintainability:** one module per stage, central config, unit tests for the cleaning rules.

## Limitations and improvements

Single-node Spark and one PostgreSQL instance; no workflow engine; full reload of aggregates on each run.
Improvements: Airflow for scheduling and retries, incremental loads, data-quality dashboards, and
cloud storage.

## Extension to real-time processing

The same cleaning rules can be applied to a stream: a producer replays trips into Apache Kafka, Spark
Structured Streaming reads the topic, applies the validation, computes windowed hourly aggregates with a
watermark for late data, and writes each micro-batch to PostgreSQL. Checkpointing gives restart safety.
This changes the ingestion and processing stages, while storage and the database schema stay the same.

## Own work and sources

The pipeline code, tests and configuration in this repository were written for this portfolio.
Data source: NYC Taxi and Limousine Commission trip record data. Learning references (Docker, Spark,
and a Kafka streaming tutorial used to understand the real-time extension) are listed in the written report.

## Project structure

```
src/            config.py  ingest.py  process.py  load_postgres.py  run_pipeline.py
sql/init.sql    database tables
tests/          unit tests
scripts/        run_pipeline.bat (Task Scheduler)
docs/           abstract and report drafts
```

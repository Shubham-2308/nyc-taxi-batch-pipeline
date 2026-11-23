@echo off
REM Used by Windows Task Scheduler. Edit the folder below to where you cloned the project.
cd /d "C:\Users\shubh\nyc-taxi-batch-pipeline"
docker compose up -d postgres
docker compose run --rm pipeline >> logs\scheduler.log 2>&1

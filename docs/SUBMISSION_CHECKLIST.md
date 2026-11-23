# Submission checklist (do in this order)

1. Unzip, copy `.env.example` to `.env`, set a password.
2. `docker compose up -d postgres`, then `docker compose run --rm pipeline` (first run downloads data).
3. Open `data/reports/quality_report.json` and note: rows_in, rows_rejected, rows_out, hourly_rows, top rejection reasons. Note the run time from `logs/pipeline.log` or the `pipeline_runs` table.
4. Take screenshots (for your own evidence): containers running, pipeline log ending in "Pipeline finished", `SELECT count(*) FROM hourly_trip_stats;`, the Task Scheduler task.
5. Push the project to GitHub (public or shared with your tutor). Do NOT push `.env`. Put the link in a TXT file.
6. Fill the yellow placeholders in `Final_Report_DRAFT.docx`, write the reflection in your own words, export to PDF, check it is 2 pages.
7. Upload in PebblePad: abstract (50 words, `ABSTRACT.txt`), final PDF, TXT with the GitHub link, phase 1 and 2 results again, optional zip. Check the field labels in your Phase 3 template before uploading.
8. Share with Atlas only when everything is final. It cannot be edited afterwards.

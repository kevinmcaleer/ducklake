# Injestion process

This applies to Page_count as well as search_logs.log.

- [ ] download the latest data snapshot - `download_all_visits_by_day.py`
    - [ ] convert to CSV if needed (search_logs.log)
    - [ ] place in `data/raw/<source>/dt=YYYY-MM-DD/` (search_logs.log)
- [ ] ingest new data, work out what is new based on timestamp, bring this into silver/gold
- [ ] regenerate reports
- [ ] validate row counts, report generation time, and data consistency


download_all_visits_by_day.py
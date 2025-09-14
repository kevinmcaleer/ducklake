# Pipeline TODO / Status (Updated 2025-09-14)

## Current Status
- Visits (page_count) ingestion working: JSON -> flattened -> ingested; reports generating.
- Search logs raw -> converted to CSV via `convert_search_logs_to_csv.py` (regex fixed) and CSVs now populated.
- `configs/sources.yml` updated: `search_logs` now points to `_tmp_downloads/search_logs_csv/` with `format: csv` (regex parse removed).
- Pipeline refresh run aborted earlier due to missing `duckdb` module in the active environment.
- Reports exist, but search-related ones may still reflect stale / incomplete silver layer because full refresh hasn’t succeeded post-change.

## Recent Changes
1. Added script `convert_search_logs_to_csv.py` (recursive, handles nested dt/run folders).
2. Fixed regex to match actual log pattern: `INFO:root:TIMESTAMP - IP: X - Query: Y`.
3. Regenerated all historical search log CSVs; verified non-empty.
4. Reconfigured `search_logs` source to use CSV ingestion (no regex parse step needed now).
5. Identified environment gap: `duckdb` Python package not installed (ModuleNotFoundError on pipeline run).

## Outstanding Issues / Gaps
- `duckdb` package missing in current virtualenv -> blocks `intake.py refresh`.
- Silver layer for `search_logs` not rebuilt since config switch (reports may mismatch).
- Need consistency check: `reports/searches_today.csv` vs `reports/searches_today_totals.csv` after successful refresh.
- Manifest may still have old `format=log` entries; confirm they don’t cause confusion or remove them (optional cleanup).
- No explicit dependency file including `duckdb` (ensure `requirements-dev.txt` or a new `requirements.txt` lists it).
- Report generation appears slow; need to profile and isolate bottlenecks (I/O vs SQL execution vs view recreation).

## Next Actions (Immediate)
1. Install dependencies (at minimum: `duckdb`, plus `pyyaml`, `tqdm`, `colorama`, `requests` if not present).
2. Rerun: `python intake.py refresh` (after venv activation) to rebuild bronze (search_logs now CSV), silver, gold, and reports.
3. Validate row counts:
   - `SELECT COUNT(*) FROM silver_search_logs;`
   - Compare with number of lines across generated CSVs.
4. Validate today’s searches consistency:
   - `searches_today.csv` aggregated sum of counts should equal `searches_today_totals.csv` value for today.
5. Spot check a few timestamps parsed from CSV align with expected UTC (no timezone skew).
6. Commit updated artifacts if pipeline succeeds.
7. Profile slow report step (capture wall time around `create_report_views` and `run_reports_sql`).

## Follow-Up / Enhancements
- Add automated ingestion task / cron wrapper script.
- Introduce simple data quality checks (e.g. non-null timestamp, reasonable IP format) pre-silver.
- Add unit test for log -> CSV conversion regex.
- Parameterize `_tmp_downloads` path (env var) for portability.
- Add retry / backoff for network downloads (page_count source timeout observed).
- Consider dedupe logic for repeated search log runs (same dt/run combination).
- Add GeoIP enrichment once base pipeline stable.
- Add lightweight timing/profiling utility for each pipeline phase (bronze, silver, gold, reports) with cumulative stats.

## Nice-to-Haves / Stretch
- Create a lightweight `Makefile` or task runner: `make refresh`, `make reports`.
- Add `README` section documenting new search_logs CSV workflow.
- Implement incremental load detection (skip unchanged CSVs by checksum before bronze ingest).
- Add simple web dashboard (DuckDB + static HTML) for quick visual checks.

## Quick Dependency Install (example)
```
pip install duckdb pyyaml tqdm colorama requests
```

## Verification Checklist After Next Refresh
- [ ] `duckdb` import succeeds
- [ ] `silver_search_logs` row count > 0
- [ ] Today’s searches consistent (detail vs totals)
- [ ] No NULL timestamps in latest partition
- [ ] Reports regenerated without errors
- [ ] Report phase wall time recorded and within acceptable threshold (< X sec TBD)

---
Document owner: pipeline automation assistant.
Update cadence: after each structural change or ingestion fix.
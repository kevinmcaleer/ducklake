# Simplified Ducklake Pipeline

Date: 2025-09-15
Status: Adopted (initial implementation)
Owner: Data Engineering

## 1. Goals
- Reduce complexity: remove multi-layer (bronze/silver/gold + heavy view recreation) overhead.
- Make ingestion idempotent and fast for daily CSV data.
- Persist efficient columnar (Parquet) partitions for direct analytical querying.
- Produce core reports quickly (<20s full, <5s incremental) with minimal intermediate objects.

## 2. Architecture Overview
```
raw CSV  -->  lake parquet  -->  daily aggregates  -->  reports
(data/raw)    (data/lake)        (DuckDB tables)       (reports/*.csv)
```

### Directories
- `data/raw/<source>/dt=YYYY-MM-DD/*.csv`  (immutable ingested inputs)
- `data/lake/<source>/dt=YYYY-MM-DD.parquet` (one parquet partition per day per source)
- `data/agg/` (optional future location for materialized aggregate parquet files; currently aggregates live as DuckDB tables)
- `reports/` (exported CSV report artifacts + summary JSON)
- `meta/` (future: schema snapshots, drift logs)

### Sources (initial)
- `page_count`
- `search_logs`

## 3. Key Components
### processed_files Table
Tracks ingested raw files and prevents duplicates.
```
processed_files(
  source TEXT,
  dt DATE,
  path TEXT,
  file_id TEXT PRIMARY KEY,  -- sha256(path + size + mtime)
  rows BIGINT,
  ingested_at TIMESTAMP
)
```

### Lake Views
Created per source over Parquet partitions:
```
CREATE OR REPLACE VIEW lake_page_count AS
  SELECT * FROM read_parquet('data/lake/page_count/dt=*.parquet');
```
(Same pattern for `lake_search_logs`).

### Daily Aggregate Tables
```
page_views_daily(dt DATE PRIMARY KEY, views BIGINT, uniq_ips BIGINT)
searches_daily(dt DATE, query TEXT, cnt BIGINT, PRIMARY KEY (dt, query))
```
Incrementally populated (append new dt only).

## 4. Ingestion Flow
1. Discover candidate CSVs in `data/raw/<source>/dt=*`.
2. Compute file_id; skip if already processed.
3. For each new file: append rows into that day’s parquet partition file (`dt=YYYY-MM-DD.parquet`).
4. Record metadata in `processed_files`.
5. Rebuild / reuse lake_* views (cheap).
6. Update daily aggregates for new dates only.
7. Generate report CSVs.

## 5. Bootstrap Strategy
A one-time export converts existing `silver_page_count` and `silver_search_logs` tables into the raw directory layout:
```
python intake.py bootstrap-raw
```
Generates per-day CSV files, enabling the simplified pipeline to operate only from raw files going forward.

## 6. Reports Implemented
| Report | Source Logic |
|--------|--------------|
| busiest_days_of_week.csv | SUM views per weekday from `page_views_daily` |
| busiest_hours_utc.csv | COUNT events by hour (on-the-fly from `lake_page_count`) |
| visits_pages_daily.csv | Daily views from `page_views_daily` |
| searches_daily.csv | Query counts from `searches_daily` |
| top_queries_all_time.csv | Aggregate over `searches_daily` |
| top_queries_30d.csv | Last 30 days window over `searches_daily` |

Additional reports (trending, engagement, etc.) can be added by extending the `REPORT_QUERIES` dict in `simple_pipeline.py`.

## 7. Performance Targets
- Bootstrap (first run): dominated by Parquet creation; acceptable once-only cost.
- Incremental daily run: file discovery + single-day parquet write + aggregate insert + report generation < 5s expected for modest data volumes.

## 8. Extensibility
To add a new source:
1. Place CSVs under `data/raw/<new_source>/dt=YYYY-MM-DD/*.csv`.
2. Add source name to `SOURCES` list in `simple_pipeline.py`.
3. (Optional) Add corresponding reports referencing a new aggregate or direct lake view.

## 9. Error Handling & Idempotency
- Duplicate ingestion prevented by `processed_files` file_id primary key.
- Re-running `simple-refresh` when no new files results in no data change and only fast report regeneration.

## 10. Future Enhancements
- Schema drift detection (store JSON schema snapshot per source and compare).
- File compaction: consolidate many small daily partitions into monthly parquet files if needed.
- Materialized trending queries if runtime > threshold.
- Optional hourly aggregates if required by downstream consumers.
- Data quality checks (row count anomalies, null ratio thresholds) before aggregate updates.

## 11. Migration Notes
Legacy components retained temporarily:
- `create_report_views` and rollup tables (can be deprecated after validation).
- Manifests & bronze/silver pipeline (superseded by simplified raw->lake flow).

Removal plan: Once simplified pipeline validated for N days without regression, remove unused commands and modules, updating README accordingly.

## 12. Command Summary
```
python intake.py bootstrap-raw      # one-time raw CSV export from silver tables
python intake.py simple-refresh     # ingest new raw CSVs, update aggregates, run reports
```

## 13. Validation Checklist
- [ ] All historical dates exported to raw.
- [ ] Parquet partitions exist for each dt (after first simple-refresh).
- [ ] daily aggregates row counts match distinct dt counts from lake views.
- [ ] Reports generated with non-zero row counts where expected.

---
End of document.

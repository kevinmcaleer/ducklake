# Ducklake (Simplified DuckDB Pipeline)

Lightweight analytics pipeline focused on two append-only sources: `page_count` and `search_logs`.

Pipeline flow:
```
data/raw/<source>/dt=YYYY-MM-DD/*.csv  ->  data/lake/<source>/dt=YYYY-MM-DD.parquet  -> daily aggregates  -> reports/*.csv
```

Enrichment: A silver-style view (`silver_page_count`) is built inline, parsing `user_agent` strings into device / OS / browser / bot fields for reporting.

## TLDR: Quick Fetch & Update

**Fetch latest data from both sources (last 3 days) and update reports:**
```bash
python run_refresh.py --db contentlake.ducklake fetch --days 3 --refresh --json
```

**Fetch only page_count (recent visits):**
```bash
python run_refresh.py --db contentlake.ducklake fetch --sources page_count --days 3 --refresh --json
```

**Fetch only search_logs:**
```bash
python run_refresh.py --db contentlake.ducklake fetch --sources search_logs --refresh --json
```

**Update reports from existing data (no fetch):**
```bash
python run_refresh.py --db contentlake.ducklake --json
```

**Auto-fetch page_count when running refresh (if data is missing):**
```bash
python run_refresh.py --db contentlake.ducklake --auto-fetch-days 2 --json
```

## Key Directories
```
data/raw/            # Input CSVs (partitioned by dt=... folders)
data/lake/           # Per-day parquet partitions
silver/page_count/   # Enriched parquet (single file) powering silver_page_count view
reports/             # Generated CSV reports + JSON summaries
ducklake_core/       # Core pipeline code (simple_pipeline.py, anomaly detection, utilities)
```

## Generated Tables / Views
- Tables: `processed_files`, `page_views_daily`, `searches_daily`, `ingestion_state`
- Views: `lake_page_count`, `lake_search_logs`, `silver_page_count`
- Reports include (non‑exhaustive): `page_views_by_agent_os_device.csv`, `visits_pages_daily.csv`, `searches_daily.csv`, `top_queries_30d.csv` etc.

## Quick Start
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python - <<'PY'
import duckdb
import pathlib
from ducklake_core.simple_pipeline import simple_refresh
conn = duckdb.connect('contentlake.ducklake')
print(simple_refresh(conn))
PY
```
Populate `data/raw/page_count/dt=YYYY-MM-DD/*.csv` and `data/raw/search_logs/dt=YYYY-MM-DD/*.csv` before running for meaningful results.

### Built-in Page Count Fetch Endpoint
The pipeline now has a hard-coded default base URL for `page_count` collection:

```
http://page_count.kevsrobots.com/all-visits
```

Fetch logic hierarchy for recent pulls (`fetch_page_count_recent`):
1. Try: `?range={N}d&since={OLDEST_DATE}` (e.g. `?range=3d&since=2025-09-24`) – returns a JSON wrapper or array containing multiple days.
2. Fallback (legacy): `?recent=N` if the above yields no rows or errors.
3. Fallback to local archives: `_tmp_downloads/all-visits-*.jsonl` (last N files) if API produced zero rows.

For full history (`fetch_page_count_all`):
1. Try: `?range=all`
2. Fallback: append `all=1` (legacy full export style)
3. Fallback: ingest every local `_tmp_downloads/all-visits-*.jsonl` file

Diagnostics now emitted in fetch JSON output:
```jsonc
"api": {
	"attempted": true,
	"attempted_urls": ["http://...range=3d&since=2025-09-24"],
	"status_code": 200,
	"rows_before_flatten": 1,
	"rows_after_flatten": 53110
},
"fallback": { "used": false, "files_considered": 0 },
"observed_dates": ["2025-09-24", "2025-09-25", "2025-09-26"],
"missing_recent_dates": [],
"freshness_gap_days": 0
```
This makes stale data root cause analysis immediate (API vs fallback vs absence of new archives).

## Adding Data
- Drop daily CSV(s) for a source into: `data/raw/<source>/dt=2025-09-18/yourfile.csv`
- Re-run `simple_refresh` (see below).

## Running the Pipeline
Minimal invocation:
```python
import duckdb
from ducklake_core.simple_pipeline import simple_refresh
conn = duckdb.connect('contentlake.ducklake')
print(simple_refresh(conn))
```

Returned dict keys:
- `new_files`: count of newly ingested raw CSV files
- `latest_page_dt` / `latest_search_dt`: most recent dates in aggregates
- `validation`: lightweight health indicators (also written to `reports/simple_validation.json`)
- `anomalies`: counts of detected anomalies per series
- `timings`: phase timing breakdown (ingest, lake_views, silver_enrich, aggregates, reports, validation, anomaly, total)

Artifacts written:
- Enriched view: `silver_page_count` (not persisted as table; rebuilt each run)
- Aggregates tables: `page_views_daily`, `searches_daily`
- CSV reports: under `reports/`
- JSON summaries: `reports/simple_refresh_summary.json`, `reports/simple_validation.json`

### Example end-to-end
```bash
python - <<'PY'
import duckdb, pathlib
from ducklake_core.simple_pipeline import simple_refresh
root = pathlib.Path('.')
raw_pc = root/'data'/'raw'/'page_count'/'dt=2025-09-18'; raw_pc.mkdir(parents=True, exist_ok=True)
raw_sl = root/'data'/'raw'/'search_logs'/'dt=2025-09-18'; raw_sl.mkdir(parents=True, exist_ok=True)
(raw_pc/'sample.csv').write_text('timestamp,ip,url,user_agent\n2025-09-18T10:00:00Z,1.1.1.1,/home,SomeBrowser/1.0\n')
(raw_sl/'searches.csv').write_text('timestamp,ip,query\n2025-09-18T10:05:00Z,1.1.1.1,Test Query\n')
con = duckdb.connect('contentlake.ducklake')
print(simple_refresh(con))
PY
```

### Command-Line Helper
You can also run the pipeline via the provided CLI script:
```bash
python run_refresh.py --db contentlake.ducklake    # basic refresh
python run_refresh.py --db contentlake.ducklake --json

# Snapshot ingest then refresh

# Important: global flags like --db must appear BEFORE the subcommand.
python run_refresh.py --db contentlake.ducklake snapshot \
	--source search_logs \
	--snapshot data/snapshots/search_logs_snapshot.csv \
	--time-col timestamp

# (Ensure the snapshot file exists; the CLI now emits a friendly error if it does not.)
```

## Incremental Snapshot Ingestion (Search Logs)
If search logs arrive as a *cumulative* snapshot (single growing CSV) rather than daily shards, use `incremental_snapshot_ingest` to extract only new rows and partition them by day:
```python
from ducklake_core.simple_pipeline import incremental_snapshot_ingest, simple_refresh
import duckdb
con = duckdb.connect('contentlake.ducklake')
res = incremental_snapshot_ingest(con, 'search_logs', 'data/snapshots/search_logs_snapshot.csv', time_col='timestamp')
print(res)
print(simple_refresh(con))  # builds views, aggregates, reports
```

## Bootstrapping From Existing Silver (Optional)
If you previously had a populated `silver_*` layer (legacy) and want to seed `data/raw` or `data/lake` directly:
- `bootstrap_raw_from_silver`: exports legacy silver tables to raw dt-partitioned CSV
- `fast_bootstrap_lake_from_silver`: writes parquet partitions straight to `data/lake/`

These helpers remain available in `simple_pipeline.py` for transitional migrations but are not required in a clean install.

## User Agent Enrichment
If `user_agents` library is installed it provides richer parsing; otherwise a lightweight heuristic fallback supplies:
`agent_type, os, os_version, browser, browser_version, device, is_mobile, is_tablet, is_pc, is_bot`.

View can be inspected:
```sql
DESCRIBE silver_page_count;
SELECT agent_type, os, device, count(*) FROM silver_page_count GROUP BY 1,2,3 ORDER BY 4 DESC;
```

## Reports

### SQL-Based Report Generation
Reports are now generated from individual SQL files in `sql/reports/`. Each report:
- Creates a CSV file in `reports/`
- Creates a corresponding DuckDB view (prefixed with `v_`) for dashboard integration
- Can be customized by editing the SQL file directly

### Available Reports
Key reports include:
- `page_views_by_agent_os_device.csv` → view: `v_page_views_by_agent_os_device`
- `visits_pages_daily.csv` → view: `v_visits_pages_daily` 
- `searches_daily.csv` → view: `v_searches_daily`
- `top_queries_all_time.csv` → view: `v_top_queries_all_time`
- `top_queries_7d.csv`, `top_queries_30d.csv` → views: `v_top_queries_7d`, `v_top_queries_30d`
- `searches_summary.csv` → view: `v_searches_summary`
- `queries_quality.csv` → view: `v_queries_quality`

### Dashboard Integration
Connect Metabase or other BI tools to the DuckDB database and query the `v_*` views:
```sql
-- Example dashboard queries
SELECT * FROM v_page_views_by_agent_os_device;
SELECT * FROM v_top_queries_7d LIMIT 10;
SELECT * FROM v_visits_pages_daily WHERE dt >= '2025-09-01';
```

### Adding Custom Reports
1. Create a new `.sql` file in `sql/reports/`
2. Add your SQL query (it will automatically create both CSV and view)
3. Run `simple_refresh()` to generate the report

Example custom report (`sql/reports/my_custom_report.sql`):
```sql
-- Daily page views with day-of-week analysis
-- Creates view: v_my_custom_report
SELECT 
  dt,
  views,
  strftime(dt, '%w') AS day_of_week,
  CASE strftime(dt, '%w') 
    WHEN '0' THEN 'Sunday'
    WHEN '6' THEN 'Saturday' 
    ELSE 'Weekday' 
  END AS day_type
FROM page_views_daily 
ORDER BY dt;
```

### Report Examples
```bash
# View generated CSV reports
head reports/page_views_by_agent_os_device.csv
head reports/top_queries_7d.csv

# Query views directly in DuckDB
duckdb contentlake.ducklake "SELECT * FROM v_searches_summary;"
```

## Anomaly Detection
Basic series anomaly marking retained (see `ducklake_core/anomaly.py`) and invoked inside `simple_refresh`.

## Development
- Keep new logic inside `ducklake_core/simple_pipeline.py`.
- No legacy manifest / bronze / gold code remains (purged for clarity; recoverable via git history if needed).

## Testing

### Running Tests
Comprehensive test suite covers SQL reports, views, and data integrity:
```bash
pip install -r requirements-dev.txt
pytest -v tests/

# Run specific test categories
pytest tests/test_sql_reports.py -v                    # SQL reports and views
pytest tests/test_sql_reports.py::TestReportViews -v  # Just view creation tests
```

### Test Coverage
- **SQL Report Loading**: Validates all SQL files can be parsed
- **View Creation**: Ensures each report creates a working DuckDB view
- **CSV Generation**: Confirms reports produce valid CSV output
- **Data Integrity**: Validates business logic and calculations
- **Report Structure**: Checks expected columns and data types

### Adding Tests for New Reports
When adding custom reports, extend the test suite:
```python
# In tests/test_sql_reports.py
def test_my_custom_report_structure(self, test_db):
    """Test my custom report has expected columns."""
    sql = "SELECT iso_week, total_views FROM v_my_custom_report LIMIT 1"
    result = test_db.execute(sql).fetchone()
    assert result is not None
    # Add specific validations...
```

## Troubleshooting

### Data Issues
- **Empty reports**: Confirm raw CSVs exist in correct `dt=...` path and contain expected columns (`page_count` needs `url,ip,user_agent,timestamp`).
- **Missing enrichment columns**: Ensure `user_agents` installed or fallback will produce simplified values.
- **Stale aggregates**: Remove a partition parquet in `data/lake/<source>/` to force rebuild from raw.

### SQL Report Issues
- **Report not generating**: Check that `.sql` file exists in `sql/reports/` and has valid SQL syntax.
- **View creation failed**: Check DuckDB logs - views require all referenced tables/columns to exist.
- **Custom report errors**: Test your SQL directly in DuckDB CLI before adding to `sql/reports/`.

### Testing SQL Reports
```bash
# Test individual SQL files
duckdb contentlake.ducklake < sql/reports/my_report.sql

# Check view was created
duckdb contentlake.ducklake "SELECT * FROM v_my_report LIMIT 1;"

# Run report tests
pytest tests/test_sql_reports.py::TestSQLReports::test_load_sql_reports -v
```

### View Debugging
```sql
-- List all views
SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW';

-- Check view definition
SELECT sql FROM sqlite_master WHERE type = 'view' AND name = 'v_my_report';
```

## Extending
### Adding a New Source
1. Create daily raw folder(s): `data/raw/my_source/dt=YYYY-MM-DD/*.csv`
2. Ensure each CSV has a timestamp (or dt) column + any dimensions needed.
3. Add the source name to `SOURCES` list in `ducklake_core/simple_pipeline.py`.
4. (Optional) Add aggregation SQL & report queries (extend `REPORT_QUERIES`).
5. Run `simple_refresh` to ingest and generate outputs.

### Adding a Custom Report
Create a new SQL file in `sql/reports/`:
```bash
# Create the SQL file
cat > sql/reports/my_custom_report.sql << 'EOF'
-- My custom weekly summary report
-- Creates view: v_my_custom_report
SELECT 
  strftime(dt, '%G-W%V') AS iso_week,
  sum(views) AS total_views,
  avg(views) AS avg_daily_views,
  count(*) AS days_in_week
FROM page_views_daily
GROUP BY 1 
ORDER BY 1;
EOF
```
Re-run `simple_refresh()` and inspect:
- CSV: `reports/my_custom_report.csv`
- DuckDB view: `v_my_custom_report`

```bash
# Check the generated report and view
head reports/my_custom_report.csv
duckdb contentlake.ducklake "SELECT * FROM v_my_custom_report LIMIT 5;"
```

## Dashboard Integration

### Built-in Views for BI Tools
All reports automatically create DuckDB views (prefixed with `v_`) that can be connected to any BI tool:

**Metabase Setup:**
1. Add DuckDB database connection pointing to `contentlake.ducklake`
2. Query views directly: `SELECT * FROM v_top_queries_7d`
3. Create dashboards using the pre-built views

**Key Dashboard Views:**
```sql
-- Page traffic analysis
SELECT * FROM v_visits_pages_daily;
SELECT * FROM v_busiest_hours_utc;
SELECT * FROM v_page_views_by_agent_os_device;

-- Search analytics
SELECT * FROM v_top_queries_7d;
SELECT * FROM v_searches_summary;
SELECT * FROM v_queries_quality;

-- Trend analysis
SELECT * FROM v_visits_pages_wow;  -- Week-over-week growth
```

### Optional Dash Dashboard
A legacy Dash dashboard (`dashboard.py`) provides basic pie charts:
```bash
pip install -r requirements-dev.txt  # includes dash + plotly
python run_refresh.py --db contentlake.ducklake  # ensure fresh data
python dashboard.py --db contentlake.ducklake --port 8050
```
Open http://127.0.0.1:8050 in your browser.

**Recommendation**: Use the DuckDB views with modern BI tools (Metabase, Grafana, etc.) for better functionality.
Add a new source by replicating the dt-partitioned raw folder convention, creating its parquet via ingestion logic adaptation, then extending aggregates/report SQL.

## TL;DR: Reset, Fetch Latest, Rebuild & Validate

Common one-liners and short flows for daily ops or a full nuke-and-reseed.

### Full Destructive Rebuild (from raw / backfill)

End-to-end rebuild using local or remote sources. Adjust paths as needed.

```bash
# 0) (Optional) Activate your venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# 1) Reset everything (DB, lake, silver, reports, and raw)
python run_refresh.py reset --include-raw

# 2) Backfill from sources
# 2a) Page Count: API full export if configured, else fall back to local JSONL archives
#     Provide API base via --page-count-url and key via --page-count-key, or set env vars
#     Fallback will scan _tmp_downloads/ for all-visits-*.jsonl if API not set

# 2b) Search Logs: from local Search-Logs.log file
#     Replace the path with your actual location
python run_refresh.py --db contentlake.ducklake backfill \
	--search-logs-file "/Volumes/Search Logs/Search-Logs.log" \
	--json

# (If you have a snapshot URL instead of a local file)
# python run_refresh.py --db contentlake.ducklake backfill \
#   --search-logs-url https://example.com/search-logs.jsonl \
#   --json

# 3) Refresh to rebuild views, aggregates, and reports (if you used --no-refresh earlier)
python run_refresh.py --db contentlake.ducklake --json

# 4) Validate outputs
head reports/searches_summary.csv
head reports/visits_pages_daily.csv
cat reports/simple_validation.json | jq '.'
cat reports/simple_refresh_summary.json | jq '.'

# 5) Optional: spot-check search quality & top queries
head reports/queries_quality.csv
head reports/top_queries_7d.csv

```

### Fetch Just the Latest (Incremental)
If you only want the most recent day(s) (page_count recent + optional search snapshot) and then refresh:

```bash
# Fetch last 3 days for BOTH sources (default) then refresh
python run_refresh.py --db contentlake.ducklake fetch \
  --days 3 \
  --refresh \
  --json

# To fetch only one source (e.g. page_count):
# python run_refresh.py --db contentlake.ducklake fetch --sources page_count --days 3 --refresh --json
```

### Force Re-import Last N Days
Rebuild only the trailing window (e.g., correct late-arriving data or parser fixes) without touching historical partitions:

```bash
# Re-ingest only last 7 days for both sources
python run_refresh.py --reimport-last-days 7 --force --force-sources page_count search_logs --db contentlake.ducklake --json
```

### Auto-Fetch Missing Recent Days
Use `--auto-fetch-days N` (or `--auto-fetch-days all`) on a plain refresh to pull recent `page_count` raw data just-in-time before building aggregates.

```bash
# Fetch last 2 days of page_count automatically, then refresh pipeline
python run_refresh.py --db contentlake.ducklake --auto-fetch-days 2 --json

# Fetch ALL historical page_count (API full export fallback / local archives) then refresh
python run_refresh.py --db contentlake.ducklake --auto-fetch-days all --json

Auto-fetch uses the same hierarchy (range/since -> recent= -> local archives). To override the default base URL supply `--page-count-url` or set `PAGE_COUNT_API_URL`.
```

### Snapshot-Based Search Logs Incremental
When search logs are a cumulative snapshot file you periodically overwrite:

```bash
python run_refresh.py snapshot \
	--db contentlake.ducklake \
	--source search_logs \
	--snapshot data/snapshots/search_logs_snapshot.csv \
	--time-col timestamp \
	--json
```

### Debug A Specific Day
```bash
# Inspect raw and lake partitions
duckdb contentlake.ducklake \
	"SELECT * FROM read_csv_auto('data/raw/search_logs/dt=2025-09-23/*.csv') LIMIT 5;" \
	"SELECT * FROM lake_search_logs WHERE date(timestamp)='2025-09-23' LIMIT 5;" 
```
```

Notes:
- The backfill step auto-discovers and ingests page_count history: API full export when configured; otherwise it imports any local `_tmp_downloads/all-visits-*.jsonl` files.
- Search logs parsing supports JSON/CSV and plain text patterns (e.g., `INFO:root:TIMESTAMP - IP: X - Query: Y`).
- Query hygiene: reports filter literal `null` and single-character queries; normalization collapses whitespace and lowercases terms.
 - Use `--force` plus `--force-sources page_count` (or `search_logs`) to surgically clear just one source while preserving the other.
 - `--rebuild-lake` with `--force` physically deletes parquet partitions so they are regenerated from raw.
 - For performance on large historical sets, rely on `backfill` once then future daily `fetch --days 1 --refresh` runs.

## License
See repository license file (if present). All legacy layers removed intentionally for simplicity.

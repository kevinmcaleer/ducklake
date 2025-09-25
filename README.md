# Ducklake (Simplified DuckDB Pipeline)

Lightweight analytics pipeline focused on two append-only sources: `page_count` and `search_logs`.

Pipeline flow:
```
data/raw/<source>/dt=YYYY-MM-DD/*.csv  ->  data/lake/<source>/dt=YYYY-MM-DD.parquet  -> daily aggregates  -> reports/*.csv
```

Enrichment: A silver-style view (`silver_page_count`) is built inline, parsing `user_agent` strings into device / OS / browser / bot fields for reporting.

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
Stored under `reports/`. Regeneration overwrites existing files. Example:
```bash
head reports/page_views_by_agent_os_device.csv
```

## Anomaly Detection
Basic series anomaly marking retained (see `ducklake_core/anomaly.py`) and invoked inside `simple_refresh`.

## Development
- Keep new logic inside `ducklake_core/simple_pipeline.py`.
- No legacy manifest / bronze / gold code remains (purged for clarity; recoverable via git history if needed).

## Testing (Minimal)
Install dev requirements then add tests for enrichment or aggregates as needed:
```bash
pip install -r requirements-dev.txt
pytest -q
```

## Troubleshooting
- Empty reports: confirm raw CSVs exist in correct dt=... path and contain expected columns (`page_count` needs `url,ip,user_agent,timestamp`).
- Missing enrichment columns: ensure `user_agents` installed or fallback will produce simplified values.
- Stale aggregates: remove a partition parquet in `data/lake/<source>/` to force rebuild from raw.

## Extending
### Adding a New Source
1. Create daily raw folder(s): `data/raw/my_source/dt=YYYY-MM-DD/*.csv`
2. Ensure each CSV has a timestamp (or dt) column + any dimensions needed.
3. Add the source name to `SOURCES` list in `ducklake_core/simple_pipeline.py`.
4. (Optional) Add aggregation SQL & report queries (extend `REPORT_QUERIES`).
5. Run `simple_refresh` to ingest and generate outputs.

### Adding a Custom Report
Edit `REPORT_QUERIES` dict in `simple_pipeline.py`:
```python
REPORT_QUERIES['my_custom_report.csv'] = """
SELECT dt, sum(views) AS total
FROM page_views_daily
GROUP BY 1 ORDER BY 1
"""
```
Re-run `simple_refresh` and inspect `reports/my_custom_report.csv`.

## Dashboard (Optional)
An optional Dash dashboard (`dashboard.py`) provides three pie charts:
- Visits by OS
- Visits by Agent Type (human vs bot)
- Visits by Device Type

Install dev extras and run:
```bash
pip install -r requirements-dev.txt  # includes dash + plotly
python run_refresh.py --db contentlake.ducklake  # ensure fresh enriched data
python dashboard.py --db contentlake.ducklake --port 8050
```
Open http://127.0.0.1:8050 in your browser.
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
# Fetch page_count last 3 days (and search logs snapshot if configured) then refresh
python run_refresh.py --db contentlake.ducklake fetch \
	--sources page_count search_logs \
	--days 3 \
	--refresh \
	--json

# (Single-line equivalent)
# python run_refresh.py --db contentlake.ducklake fetch --sources page_count search_logs --days 3 --refresh --json
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

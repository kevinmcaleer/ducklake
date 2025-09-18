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
python run_refresh.py snapshot \
	--db contentlake.ducklake \
	--source search_logs \
	--snapshot data/snapshots/search_logs_snapshot.csv \
	--time-col timestamp
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

## License
See repository license file (if present). All legacy layers removed intentionally for simplicity.

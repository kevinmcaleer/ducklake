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
```python
import duckdb
from ducklake_core.simple_pipeline import simple_refresh
conn = duckdb.connect('contentlake.ducklake')
result = simple_refresh(conn)
print(result)
```
Outputs:
- Enriched view `silver_page_count`
- Updated daily aggregates
- Fresh CSVs in `reports/`
- Validation JSON: `reports/simple_validation.json`
- Summary JSON: `reports/simple_refresh_summary.json`
- Anomaly summary counts embedded in returned dict.

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
Add a new source by replicating the dt-partitioned raw folder convention, creating its parquet via ingestion logic adaptation, then extending aggregates/report SQL.

## License
See repository license file (if present). All legacy layers removed intentionally for simplicity.

# Ducklake: Lightweight DuckDB Lakehouse

This repo organizes your data into bronze/silver/gold folders and a DuckDB database for quick analytics, with optional GeoIP enrichment.

## Project Structure

```
bronze/         # Raw immutable files by source/format/dt/run
silver/         # Typed, deduped, partitioned Parquet per source (views: silver_<source>)
gold/           # Rollups (daily, all-time) (views: gold_<source>_daily, gold_<source>_all_time)
configs/        # Source configs (sources.yml)
data/geoip/     # GeoIP CSVs (GeoLite2)
reports/        # Output CSV reports
duckdb/lake.duckdb  # Main DuckDB database
intake.py       # CLI for ingest/silver/gold/refresh
run_imports.py  # Orchestrator for full pipeline
queries.sql     # Ad-hoc analysis queries
reports.sql     # CSV export scripts
geoip_searches_by_country.sql # Example: search logs by country
```

## Workflow

### Simplified Pipeline (New)

For append-only daily sources (`page_count`, `search_logs`) a faster minimal path is available:

```
raw CSV  ->  lake parquet (one dt=YYYY-MM-DD.parquet per source/day)  -> daily aggregates  -> reports/*.csv
```

Artifacts:
- `data/raw/<source>/dt=YYYY-MM-DD/*.csv` (ingestion inputs)
- `data/lake/<source>/dt=YYYY-MM-DD.parquet` (columnar partitions)
- Tables: `page_views_daily`, `searches_daily`
- Views: `lake_page_count`, `lake_search_logs`
- Reports + summaries: `reports/*.csv`, `reports/simple_refresh_summary.json`, `reports/anomalies.json`

Commands:
```bash
python intake.py bootstrap-raw        # one-time export from existing silver tables
python intake.py simple-refresh       # ingest new raw -> parquet -> aggregates -> reports
python intake.py simple-validate      # validation only (no ingestion)
python intake.py fast-bootstrap-lake  # direct parquet export from silver (faster historical seed)
```

Makefile shortcuts:
```bash
make simple-refresh
make validate
make bootstrap-raw
make fast-bootstrap-lake
make cleanup-manifest
```

Performance: target <5s incremental; timings + anomaly detection included in output JSON.

Migration: run legacy `refresh` alongside simplified mode until validated (see `design/simplified_pipeline.md`).

### 1. Activate the venv

```bash
source .venv/bin/activate
```

### 2. Ingest data (bronze)

- Local file:
  ```bash
  ```
- URL:
  ```bash
  ```
- SQLite:
  ```bash
  ```

### 3. Build silver and gold

```bash
python intake.py silver page_count
python intake.py gold page_count --title-col url --agg count
```
Or refresh all sources:
```bash
python intake.py refresh
```

### 4. Orchestrate everything (recommended)

```bash
python run_imports.py --reports
```
This will ingest, build silver/gold, and run reports for all sources in `configs/sources.yml`.

### 5. Query and analyze

- In Python:
  ```python
  ```
- Ad-hoc SQL:
  ```bash
  ```
- Export CSV reports:
  ```bash
  ```

## GeoIP Enrichment

### Running tests

Install dev dependencies and run the tests:

Using uv:

```sh
uv pip install -r requirements-dev.txt
pytest -q
```

Using pip:

```sh
python -m pip install -r requirements-dev.txt
pytest -q
```
1. Download GeoLite2 CSVs from MaxMind and place in `data/geoip/`.
2. Add `geoip_blocks` and `geoip_locations` to `configs/sources.yml` (see example).
3. Run:
   ```bash
   python run_imports.py --only geoip_blocks,geoip_locations
   ```
4. Use `geoip_searches_by_country.sql` to aggregate search logs by country:
   ```bash
   duckdb duckdb/lake.duckdb -c ".read geoip_searches_by_country.sql"
   ```

## Automation

- To run the full pipeline and generate reports daily (via cron):
  ```cron
  0 2 * * * cd /path/to/ducklake && /usr/bin/python3 run_imports.py --reports >> logs/cron.log 2>&1

## Troubleshooting

- If you see lock errors, remove `.ducklake.lock` if no other run is active.
- If gold rollups fail, check your config and ensure the correct columns exist in silver.
- For GeoIP, ensure the CSVs are present and configs are correct.
Define sources in `configs/sources.yml`. Example:

```yaml
page_count:
  url: "https://page_count.kevsrobots.com/all-visits"
  format: json
  expect_schema:
    url: string
    ip: string
    user_agent: string
    timestamp: datetime
  partitions: [dt]
  normalize:
    tz: UTC
```

Notes:
- `expect_schema` drives casting in silver; include a `ts` column to derive dt from it, otherwise the bronze dt is used.
- `primary_key` (optional) enables dedupe keeping the latest ts.
- All silver and gold outputs are Parquet and accessible as DuckDB views.
Define sources in `configs/sources.yml`. Example shipped:

```yaml
page_count:
  url: "https://page_count.kevsrobots.com/all-visits"
  format: json
  expect_schema:
    url: string
    ip: string
    user_agent: string
    timestamp: datetime
  partitions: [dt]
  normalize:
    tz: UTC
```

Notes:

- expect_schema drives casting in silver; include a `ts` column to derive dt from it, otherwise the bronze dt is used.
- primary_key (optional) enables dedupe keeping the latest ts.
- All silver and gold outputs are Parquet and accessible as DuckDB views.

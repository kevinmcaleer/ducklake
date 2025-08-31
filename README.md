# Ducklake: lightweight DuckDB lakehouse

This repo organizes your data into bronze/silver/gold folders and a DuckDB database for quick analytics.

- bronze: immutable raw files organized by source/format/dt/run
- silver: typed, deduped, partitioned parquet per source with a matching DuckDB view (silver_\<source>)
- gold: simple rollups (daily, all-time) with views (gold_\<source>_daily, gold_\<source>_all_time)

## Quickstart

1) Activate the venv (VS Code picks it automatically). Packages: duckdb, pyyaml, pandas.

2) Ingest data into bronze

- Local CSV/JSON/Parquet file:

```bash
./venv/bin/python intake.py ingest-file page_count path/to/file.csv --format csv --dt 2025-08-31
```

- URL (downloads and stores a snapshot under bronze):

```bash
./venv/bin/python intake.py ingest-url page_count "https://page_count.kevsrobots.com/all-visits" --format json --dt 2025-08-31
```

- SQLite table:

```bash
./venv/bin/python intake.py ingest-sqlite website_pages path/to/analytics.db events --dt 2025-08-31
```

1) Build silver and gold

```bash
./venv/bin/python intake.py silver page_count
./venv/bin/python intake.py gold page_count --title-col url --value-col views
```

Or refresh all sources from `configs/sources.yml`:

```bash
./venv/bin/python intake.py refresh
```

1) Query DuckDB views

```bash
./venv/bin/python -c "import duckdb; con=duckdb.connect('duckdb/lake.duckdb'); print(con.execute('SELECT * FROM gold_page_count_all_time LIMIT 10').fetchdf())"
```

Or use the bundled SQL scripts:

```bash
# ad-hoc analysis queries
duckdb duckdb/lake.duckdb -c ".read queries.sql"

# export CSV reports into ./reports (create the folder first)
mkdir -p reports
duckdb duckdb/lake.duckdb -c ".read reports.sql"
```

## Configs

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

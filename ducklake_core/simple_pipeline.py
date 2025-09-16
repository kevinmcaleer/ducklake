"""Simplified pipeline: raw CSV -> lake parquet -> daily aggregates -> reports.

Directory expectations:
- data/raw/<source>/dt=YYYY-MM-DD/*.csv  (input)
- data/lake/<source>/dt=YYYY-MM-DD.parquet (per-day partition written once)
- data/agg/*.parquet (daily aggregate tables persisted as DuckDB tables inside DB for now; optional parquet later)
- reports/*.csv (outputs)

Sources handled initially: page_count, search_logs.
"""
from __future__ import annotations
import duckdb, pathlib, hashlib, time, json, sys
from datetime import datetime, date

ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / 'data' / 'raw'
LAKE_DIR = ROOT / 'data' / 'lake'
REPORTS_DIR = ROOT / 'reports'

SOURCES = ['page_count', 'search_logs']

# Basic report SQL mapping (depends on aggregates)
REPORT_QUERIES = {
  'busiest_days_of_week.csv': """
    SELECT strftime(dt, '%w') AS dow_num,
           CASE strftime(dt, '%w') WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue' WHEN '3' THEN 'Wed' WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri' WHEN '6' THEN 'Sat' END AS dow,
           sum(views) AS visits
    FROM page_views_daily
    GROUP BY 1,2
    ORDER BY 1
  """,
  'busiest_hours_utc.csv': """
    SELECT hour_utc, sum(views) AS cnt
    FROM (
      SELECT date_part('hour', COALESCE(ts, timestamp)) AS hour_utc, 1 AS views
      FROM lake_page_count
      WHERE COALESCE(ts, timestamp) IS NOT NULL
    )
    GROUP BY 1 ORDER BY 1
  """,
  'visits_pages_daily.csv': "SELECT dt, views AS cnt FROM page_views_daily ORDER BY dt",
  'searches_daily.csv': "SELECT dt, query, cnt FROM searches_daily ORDER BY dt, cnt DESC",
  'top_queries_all_time.csv': """
    SELECT query, sum(cnt) AS cnt
    FROM searches_daily
    GROUP BY 1 ORDER BY cnt DESC LIMIT 100
  """,
  'top_queries_30d.csv': """
    SELECT query, sum(cnt) AS cnt
    FROM searches_daily WHERE dt >= current_date - INTERVAL 30 DAY
    GROUP BY 1 ORDER BY cnt DESC LIMIT 100
  """,
  'searches_summary.csv': """
    SELECT count(DISTINCT dt) AS days, count(*) AS query_day_rows, sum(cnt) AS total_searches,
           count(DISTINCT query) AS distinct_queries
    FROM searches_daily
  """,
  'searches_distinct_daily.csv': """
    SELECT dt, count(DISTINCT query) AS distinct_queries, sum(cnt) AS searches
    FROM searches_daily GROUP BY 1 ORDER BY 1
  """,
}


def ensure_core_tables(conn: duckdb.DuckDBPyConnection):
  conn.execute("CREATE TABLE IF NOT EXISTS processed_files (source TEXT, dt DATE, path TEXT, file_id TEXT PRIMARY KEY, rows BIGINT, ingested_at TIMESTAMP)")
  conn.execute("CREATE TABLE IF NOT EXISTS page_views_daily (dt DATE PRIMARY KEY, views BIGINT, uniq_ips BIGINT)")
  # Allow NULL query temporarily (filter out during aggregation) by not enforcing NOT NULL constraint
  conn.execute("CREATE TABLE IF NOT EXISTS searches_daily (dt DATE, query TEXT, cnt BIGINT, PRIMARY KEY (dt, query))")
  # Performance indexes (DuckDB will treat these as projections in newer versions)
  try:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_searches_daily_dt ON searches_daily(dt)")
  except Exception:
    pass
  try:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_searches_daily_query ON searches_daily(query)")
  except Exception:
    pass


def file_id(path: pathlib.Path) -> str:
  st = path.stat()
  h = hashlib.sha256()
  h.update(str(path).encode())
  h.update(str(st.st_size).encode())
  h.update(str(int(st.st_mtime)).encode())
  return h.hexdigest()


def discover_new_files() -> list[tuple[str, date, pathlib.Path]]:
  results = []
  for source in SOURCES:
    base = RAW_DIR / source
    if not base.exists():
      continue
    for dt_dir in base.glob('dt=*'):
      if not dt_dir.is_dir():
        continue
      try:
        dt_val = dt_dir.name.split('dt=')[1]
        dt_obj = date.fromisoformat(dt_val)
      except Exception:
        continue
      for csv_file in dt_dir.glob('*.csv'):
        results.append((source, dt_obj, csv_file))
  return results


def ingest_new_files(conn: duckdb.DuckDBPyConnection):
  LAKE_DIR.mkdir(parents=True, exist_ok=True)
  new = 0
  for source, dt_obj, path in discover_new_files():
    fid = file_id(path)
    row = conn.execute("SELECT 1 FROM processed_files WHERE file_id=?", [fid]).fetchone()
    if row:
      continue
    # Write per-day parquet partition file if not exists
    part_dir = LAKE_DIR / source
    part_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = part_dir / f"dt={dt_obj}.parquet"
    # If file already exists, append by reading existing + new (for simplicity rewrite)
    # Load CSV
    try:
      # Load into DuckDB ephemeral table then write (append via UNION ALL + DISTINCT if duplicates risk)
      df = conn.execute(f"SELECT * FROM read_csv_auto('{path}')").df()
      if parquet_path.exists():
        # Read existing parquet, union, drop exact duplicates
        existing = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
        import pandas as pd
        combined = pd.concat([existing, df], ignore_index=True).drop_duplicates()
        # Overwrite
        conn.execute(f"COPY (SELECT * FROM combined) TO '{parquet_path}' (FORMAT PARQUET)")
      else:
        conn.execute(f"COPY (SELECT * FROM read_csv_auto('{path}')) TO '{parquet_path}' (FORMAT PARQUET)")
      conn.execute("INSERT INTO processed_files VALUES (?,?,?,?,?,current_timestamp)", [source, dt_obj, str(path), fid, len(df)])
      new += 1
    except Exception as e:
      print(f"Failed ingest {path}: {e}", file=sys.stderr)
  return new


def create_lake_views(conn: duckdb.DuckDBPyConnection):
  # Choose directory-style partitions first (dt=*/*.parquet), fallback to flat files (dt=*.parquet)
  for source in SOURCES:
    vname = f"lake_{source}"
    base = LAKE_DIR / source
    dir_pattern = base / 'dt=*/*.parquet'
    flat_pattern = base / 'dt=*.parquet'
    if list(base.glob('dt=*/*.parquet')):
      conn.execute(
        f"CREATE OR REPLACE VIEW {vname} AS SELECT * FROM read_parquet('{dir_pattern}', union_by_name=true)"
      )
    elif list(base.glob('dt=*.parquet')):
      conn.execute(
        f"CREATE OR REPLACE VIEW {vname} AS SELECT * FROM read_parquet('{flat_pattern}', union_by_name=true)"
      )
    else:
      conn.execute(
        f"CREATE OR REPLACE VIEW {vname} AS SELECT * FROM (SELECT NULL WHERE FALSE)"
      )


def update_daily_aggregates(conn: duckdb.DuckDBPyConnection):
  # Page views daily
  max_dt_row = conn.execute("SELECT COALESCE(max(dt), DATE '1970-01-01') FROM page_views_daily").fetchone()
  max_dt = max_dt_row[0]
  conn.execute("""
    INSERT OR REPLACE INTO page_views_daily
    SELECT date(COALESCE(ts, timestamp)) AS dt,
           count(*) AS views,
           count(DISTINCT ip) AS uniq_ips
    FROM lake_page_count
    WHERE date(COALESCE(ts, timestamp)) > ?
    GROUP BY 1
  """, [max_dt])
  # Searches daily (dynamic column detection: prefer timestamp, else dt already present)
  # We'll rebuild searches for new days only; if logic changes to full rebuild set full_rebuild=True
  max_s_dt = conn.execute("SELECT COALESCE(max(dt), DATE '1970-01-01') FROM searches_daily").fetchone()[0]
  # Use DESCRIBE to get real column names; fallback to PRAGMA if needed
  try:
    cols = [r[0] for r in conn.execute("DESCRIBE lake_search_logs").fetchall()]
  except Exception:
    # PRAGMA table_info returns (cid, name, type, notnull, dflt_value, pk)
    cols = [r[1] for r in conn.execute("PRAGMA table_info('lake_search_logs')").fetchall()]
  time_col = None
  for candidate in ('timestamp', 'ts', 'event_ts'):
    if candidate in cols:
      time_col = candidate
      break
  if time_col:
    conn.execute(f"""
      INSERT OR REPLACE INTO searches_daily
      SELECT date({time_col}) AS dt,
        lower(trim(query)) AS query,
        count(*) AS cnt
      FROM lake_search_logs
      WHERE {time_col} IS NOT NULL AND query IS NOT NULL AND length(trim(query))>0 AND date({time_col}) > ?
      GROUP BY 1,2
    """, [max_s_dt])
  else:
    # Fallback: assume dt column exists already (partition produced only dt + query + ip etc.)
    if 'dt' in cols:
      conn.execute("""
        INSERT OR REPLACE INTO searches_daily
        SELECT dt, lower(trim(query)) AS query, count(*) AS cnt
        FROM lake_search_logs
        WHERE dt > ? AND query IS NOT NULL AND length(trim(query))>0
        GROUP BY 1,2
      """, [max_s_dt])


def validate_simple_pipeline(conn: duckdb.DuckDBPyConnection) -> dict:
  """Run lightweight validation checks on core pipeline objects."""
  checks = {}
  def safe(sql):
    try:
      return conn.execute(sql).fetchone()[0]
    except Exception:
      return None
  checks['lake_page_count_exists'] = safe("SELECT 1 FROM information_schema.tables WHERE table_name='lake_page_count'") == 1
  checks['lake_search_logs_exists'] = safe("SELECT 1 FROM information_schema.tables WHERE table_name='lake_search_logs'") == 1
  checks['page_views_daily_rows'] = safe("SELECT COUNT(*) FROM page_views_daily")
  checks['searches_daily_rows'] = safe("SELECT COUNT(*) FROM searches_daily")
  checks['latest_page_dt'] = safe("SELECT max(dt) FROM page_views_daily")
  checks['latest_search_dt'] = safe("SELECT max(dt) FROM searches_daily")
  # Basic freshness: ensure latest page dt within 7 days of today (if data exists)
  try:
    checks['page_data_fresh'] = conn.execute("SELECT (julianday(current_date) - julianday(COALESCE(max(dt), DATE '1970-01-01'))) <= 7 FROM page_views_daily").fetchone()[0]
  except Exception:
    checks['page_data_fresh'] = None
  (REPORTS_DIR / 'simple_validation.json').write_text(json.dumps(checks, indent=2, default=str))
  return checks


def run_simple_reports(conn: duckdb.DuckDBPyConnection):
  REPORTS_DIR.mkdir(parents=True, exist_ok=True)
  out = {}
  for fname, sql in REPORT_QUERIES.items():
    target = REPORTS_DIR / fname
    conn.execute(f"COPY ({sql}) TO '{target}' (HEADER TRUE, DELIMITER ',')")
    rows = sum(1 for _ in target.open()) - 1 if target.exists() else 0
    out[fname] = rows
  (REPORTS_DIR / 'simple_refresh_summary.json').write_text(json.dumps({'generated': out, 'ts': datetime.utcnow().isoformat()+'Z'}, indent=2))


def simple_refresh(conn: duckdb.DuckDBPyConnection):
  ensure_core_tables(conn)
  t0 = time.time()
  phases = {}
  # Ingest
  p0 = time.time(); new_files = ingest_new_files(conn); phases['ingest_s'] = round(time.time()-p0, 3)
  # Lake views
  p0 = time.time(); create_lake_views(conn); phases['lake_views_s'] = round(time.time()-p0, 3)
  # Aggregates
  p0 = time.time(); update_daily_aggregates(conn); phases['aggregates_s'] = round(time.time()-p0, 3)
  # Reports
  p0 = time.time(); run_simple_reports(conn); phases['reports_s'] = round(time.time()-p0, 3)
  # Validation
  p0 = time.time(); validate = validate_simple_pipeline(conn); phases['validation_s'] = round(time.time()-p0, 3)
  total = round(time.time() - t0, 3)
  phases['total_s'] = total
  return {
    'new_files': new_files,
    'latest_page_dt': conn.execute("SELECT max(dt) FROM page_views_daily").fetchone()[0],
    'latest_search_dt': conn.execute("SELECT max(dt) FROM searches_daily").fetchone()[0],
    'validation': validate,
    'timings': phases,
  }


def bootstrap_raw_from_silver(conn: duckdb.DuckDBPyConnection, overwrite: bool = False, verbose: bool = True) -> dict:
  """Export existing silver_* tables into data/raw/<source>/dt=YYYY-MM-DD/*.csv.

  Emits progress if verbose=True so long-running exports don't appear hung.
  """
  RAW_DIR.mkdir(parents=True, exist_ok=True)
  summary = { 'page_count': {'exported': 0, 'total_dt': 0}, 'search_logs': {'exported': 0, 'total_dt': 0} }

  def _log(msg: str):
    if verbose:
      print(msg, flush=True)

  start = time.time()
  # Page count
  try:
    dts = [r[0] for r in conn.execute("SELECT DISTINCT dt FROM silver_page_count WHERE dt IS NOT NULL ORDER BY 1").fetchall()]
    summary['page_count']['total_dt'] = len(dts)
    _log(f"[bootstrap] page_count: {len(dts)} distinct days to consider")
    for i, dtv in enumerate(dts, 1):
      out_dir = RAW_DIR / 'page_count' / f'dt={dtv}'
      out_dir.mkdir(parents=True, exist_ok=True)
      out_file = out_dir / 'page_count.csv'
      if out_file.exists() and not overwrite:
        if i % 25 == 0:
          _log(f"  page_count progress {i}/{len(dts)} (skipping existing)")
        continue
      conn.execute(f"COPY (SELECT * FROM silver_page_count WHERE dt='{dtv}') TO '{out_file}' (HEADER TRUE, DELIMITER ',')")
      summary['page_count']['exported'] += 1
      if i % 25 == 0 or summary['page_count']['exported'] <= 3:
        _log(f"  page_count exported dt={dtv} ({summary['page_count']['exported']} so far)")
  except Exception as e:
    print(f"Bootstrap page_count failed: {e}", file=sys.stderr)

  # Search logs
  try:
    dts = [r[0] for r in conn.execute("SELECT DISTINCT date(timestamp) AS dt FROM silver_search_logs WHERE timestamp IS NOT NULL ORDER BY 1").fetchall()]
    summary['search_logs']['total_dt'] = len(dts)
    _log(f"[bootstrap] search_logs: {len(dts)} distinct days to consider")
    for i, dtv in enumerate(dts, 1):
      out_dir = RAW_DIR / 'search_logs' / f'dt={dtv}'
      out_dir.mkdir(parents=True, exist_ok=True)
      out_file = out_dir / 'search_logs.csv'
      if out_file.exists() and not overwrite:
        if i % 25 == 0:
          _log(f"  search_logs progress {i}/{len(dts)} (skipping existing)")
        continue
      conn.execute(f"COPY (SELECT * FROM silver_search_logs WHERE date(timestamp)='{dtv}') TO '{out_file}' (HEADER TRUE, DELIMITER ',')")
      summary['search_logs']['exported'] += 1
      if i % 25 == 0 or summary['search_logs']['exported'] <= 3:
        _log(f"  search_logs exported dt={dtv} ({summary['search_logs']['exported']} so far)")
  except Exception as e:
    print(f"Bootstrap search_logs failed: {e}", file=sys.stderr)

  summary['duration_s'] = round(time.time() - start, 2)
  _log(f"[bootstrap] complete in {summary['duration_s']}s")
  return summary


def fast_bootstrap_lake_from_silver(conn: duckdb.DuckDBPyConnection, overwrite: bool = False, verbose: bool = True) -> dict:
  """Directly populate data/lake/<source>/dt=*.parquet from silver tables (bypasses raw CSV stage).

  Useful when seeding large historical backfill. Raw CSV bootstrap can be run later if archival CSVs are required.
  """
  LAKE_DIR.mkdir(parents=True, exist_ok=True)
  def _log(msg: str):
    if verbose:
      print(msg, flush=True)
  out = { 'page_count': 0, 'search_logs': 0 }
  start = time.time()
  # Strategy: use a single COPY with PARTITION_BY for each source (dramatically faster than per-day loop)
  # If not overwriting, we exclude already existing dt directories to avoid rewriting.
  def existing_dt_dirs(source: str):
    base = LAKE_DIR / source
    return {p.name.split('=')[1] for p in base.glob('dt=*') if p.is_dir() or p.suffix == '.parquet'}

  # Helper to decide strategy: if directory empty OR overwrite -> single partitioned COPY; else per-dt flat files.
  def dir_empty(path: pathlib.Path) -> bool:
    if not path.exists():
      return True
    return not any(path.iterdir())

  # PAGE COUNT
  try:
    base_dir = LAKE_DIR / 'page_count'; base_dir.mkdir(parents=True, exist_ok=True)
    existing = existing_dt_dirs('page_count') if not overwrite else set()
    all_dts = [r[0] for r in conn.execute("SELECT DISTINCT dt FROM silver_page_count WHERE dt IS NOT NULL ORDER BY 1").fetchall()]
    missing = [d for d in all_dts if d not in existing or overwrite]
    if overwrite and not dir_empty(base_dir):
      # Clear directory for clean partitioned write
      for p in base_dir.glob('*'): 
        if p.is_dir():
          for sub in p.glob('**/*'): sub.unlink(missing_ok=True)
          try: p.rmdir()
          except Exception: pass
        else:
          p.unlink(missing_ok=True)
    if dir_empty(base_dir) and missing:
      _log(f"[fast-bootstrap] page_count full partitioned export ({len(missing)} days)")
      conn.execute(f"COPY (SELECT * FROM silver_page_count) TO '{base_dir}' (FORMAT PARQUET, PARTITION_BY (dt))")
      out['page_count'] = len(missing)
    else:
      _log(f"[fast-bootstrap] page_count incremental export {len(missing)} missing days (existing={len(existing)})")
      for i, dtv in enumerate(missing, 1):
        # Write as flat file to avoid partitioned directory non-empty restriction
        target = base_dir / f"dt={dtv}.parquet"
        if target.exists() and not overwrite:
          continue
        conn.execute(f"COPY (SELECT * FROM silver_page_count WHERE dt='{dtv}') TO '{target}' (FORMAT PARQUET)")
        if i <= 3 or i % 50 == 0:
          _log(f"  page_count wrote dt={dtv} ({i}/{len(missing)})")
  except Exception as e:
    print(f"fast bootstrap page_count failed: {e}", file=sys.stderr)

  # SEARCH LOGS
  try:
    base_dir = LAKE_DIR / 'search_logs'; base_dir.mkdir(parents=True, exist_ok=True)
    existing = existing_dt_dirs('search_logs') if not overwrite else set()
    all_dts = [r[0] for r in conn.execute("SELECT DISTINCT date(timestamp) FROM silver_search_logs WHERE timestamp IS NOT NULL ORDER BY 1").fetchall()]
    missing = [d for d in all_dts if d not in existing or overwrite]
    if overwrite:
      for p in base_dir.glob('*'):
        p.unlink(missing_ok=True)
    _log(f"[fast-bootstrap] search_logs export mode=flat files days={len(missing)} overwrite={overwrite}")
    for i, dtv in enumerate(missing, 1):
      target = base_dir / f"dt={dtv}.parquet"
      if target.exists() and not overwrite:
        continue
      conn.execute(f"COPY (SELECT *, date(timestamp) AS dt FROM silver_search_logs WHERE date(timestamp)='{dtv}') TO '{target}' (FORMAT PARQUET)")
      if i <= 3 or i % 50 == 0:
        _log(f"  search_logs wrote dt={dtv} ({i}/{len(missing)})")
    out['search_logs'] = len(missing)
  except Exception as e:
    print(f"fast bootstrap search_logs failed: {e}", file=sys.stderr)
  out['duration_s'] = round(time.time() - start, 2)
  _log(f"[fast-bootstrap] complete in {out['duration_s']}s")
  return out

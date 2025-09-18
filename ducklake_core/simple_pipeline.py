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
from .anomaly import detect_anomalies

ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / 'data' / 'raw'
LAKE_DIR = ROOT / 'data' / 'lake'
REPORTS_DIR = ROOT / 'reports'

SOURCES = ['page_count', 'search_logs']

REPORT_QUERIES = {
  'page_views_by_agent_os_device.csv': '''
    SELECT
      agent_type,
      os,
      device,
      count(*) AS view_count
    FROM silver_page_count
    GROUP BY 1,2,3
    ORDER BY view_count DESC
  ''',
  'searches_weekly.csv': """
    SELECT strftime(dt, '%G-W%V') AS iso_week, sum(cnt) AS search_count
    FROM searches_daily
    GROUP BY 1
    ORDER BY 1
  """,
  'visits_pages_monthly.csv': """
    SELECT strftime(dt, '%Y%m') AS yyyymm, sum(views) AS cnt
    FROM page_views_daily
    GROUP BY 1
    ORDER BY 1
  """,
  'visits_pages_weekly.csv': """
    SELECT strftime(dt, '%G-W%V') AS iso_week, sum(views) AS cnt
    FROM page_views_daily
    GROUP BY 1
    ORDER BY 1
  """,
  'visits_pages_wow.csv': """
    WITH weekly AS (
      SELECT strftime(dt, '%G-W%V') AS iso_week, sum(views) AS week_cnt
      FROM page_views_daily
      GROUP BY 1
    ),
    wow AS (
      SELECT iso_week, week_cnt,
             lag(week_cnt) OVER (ORDER BY iso_week) AS prev_week,
             week_cnt - lag(week_cnt) OVER (ORDER BY iso_week) AS delta,
             CASE WHEN lag(week_cnt) OVER (ORDER BY iso_week) = 0 THEN NULL
                  ELSE round(100.0 * (week_cnt - lag(week_cnt) OVER (ORDER BY iso_week)) / lag(week_cnt) OVER (ORDER BY iso_week), 2)
             END AS pct_change
      FROM weekly
    )
    SELECT iso_week, week_cnt, prev_week, delta, pct_change FROM wow ORDER BY iso_week
  """,
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
      SELECT date_part('hour', timestamp) AS hour_utc, 1 AS views
      FROM lake_page_count
      WHERE timestamp IS NOT NULL
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
  'searches_today.csv': """
    SELECT date(l.timestamp) AS dt, l.timestamp, l.ip, lower(trim(l.query)) AS query
    FROM lake_search_logs l
    WHERE l.timestamp IS NOT NULL
      AND date(l.timestamp) = current_date
      AND l.query IS NOT NULL AND length(trim(l.query))>0
    ORDER BY l.timestamp
  """,
  'searches_today_totals.csv': """
    WITH today AS (
      SELECT date(timestamp) AS dt, lower(trim(query)) AS query
      FROM lake_search_logs
      WHERE timestamp IS NOT NULL
        AND date(timestamp) = current_date
        AND query IS NOT NULL AND length(trim(query))>0
    )
    SELECT dt,
           COUNT(*) AS raw_events,
           COUNT(DISTINCT query) AS distinct_queries,
           (SELECT COALESCE(sum(cnt),0) FROM searches_daily WHERE dt = current_date) AS aggregated_query_events,
           (SELECT COALESCE(sum(cnt),0) FROM searches_daily) AS total_searches_all_time
    FROM today
    GROUP BY 1
  """,
}


def ensure_core_tables(conn: duckdb.DuckDBPyConnection):
  conn.execute("CREATE TABLE IF NOT EXISTS processed_files (source TEXT, dt DATE, path TEXT, file_id TEXT PRIMARY KEY, rows BIGINT, ingested_at TIMESTAMP)")
  conn.execute("CREATE TABLE IF NOT EXISTS page_views_daily (dt DATE PRIMARY KEY, views BIGINT, uniq_ips BIGINT)")
  # Allow NULL query temporarily (filter out during aggregation) by not enforcing NOT NULL constraint
  conn.execute("CREATE TABLE IF NOT EXISTS searches_daily (dt DATE, query TEXT, cnt BIGINT, PRIMARY KEY (dt, query))")
  # High-water mark per source for snapshot-style incremental ingestion
  conn.execute("CREATE TABLE IF NOT EXISTS ingestion_state (source TEXT PRIMARY KEY, last_ts TIMESTAMP)")
  # Performance indexes (DuckDB will treat these as projections in newer versions)
  try:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_searches_daily_dt ON searches_daily(dt)")
  except Exception:
    pass


def _scalar(conn: duckdb.DuckDBPyConnection, sql: str, params=None):
  try:
    row = conn.execute(sql, params or []).fetchone()
    return row[0] if row and len(row) else None
  except Exception:
    return None
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
    try:
      if parquet_path.exists():
        # Align schemas by projecting the same column list from both sides
        existing_cols = [r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()]
        csv_cols = [r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{path}')").fetchall()]
        csv_set = set(csv_cols)
        select_existing = ",".join(existing_cols)
        select_new = ",".join([c if c in csv_set else f"NULL AS {c}" for c in existing_cols])
        tmp_path = parquet_path.with_suffix('.parquet.tmp')
        try:
          if tmp_path.exists():
            tmp_path.unlink()
        except Exception:
          pass
        conn.execute(
          f"COPY ((SELECT {select_existing} FROM read_parquet('{parquet_path}')) UNION ALL (SELECT {select_new} FROM read_csv_auto('{path}'))) TO '{tmp_path}' (FORMAT PARQUET)"
        )
        try:
          parquet_path.unlink()
          tmp_path.rename(parquet_path)
        except Exception as e:
          raise RuntimeError(f"Failed replacing parquet partition {parquet_path}: {e}")
        # Clean up any leftover .parquet.tmp files
        if tmp_path.exists():
          try:
            tmp_path.unlink()
          except Exception:
            pass
        added_rows = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path}')").fetchone()[0]
      else:
        conn.execute(f"COPY (SELECT * FROM read_csv_auto('{path}')) TO '{parquet_path}' (FORMAT PARQUET)")
        added_rows = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path}')").fetchone()[0]
      conn.execute("INSERT INTO processed_files VALUES (?,?,?,?,?,current_timestamp)", [source, dt_obj, str(path), fid, added_rows])
      new += 1
    except Exception as e:
      print(f"Failed ingest {path}: {e}", file=sys.stderr)
    # Always try to clean up .parquet.tmp files after each attempt
    tmp_path = parquet_path.with_suffix('.parquet.tmp')
    if tmp_path.exists():
      try:
        tmp_path.unlink()
      except Exception:
        pass
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
  # Always use COALESCE for all likely timestamp columns
  try:
    cols = [r[0] for r in conn.execute("DESCRIBE lake_page_count").fetchall()]
  except Exception:
    cols = [r[1] for r in conn.execute("PRAGMA table_info('lake_page_count')").fetchall()]
  # Build COALESCE expression for all present timestamp columns
  candidates = [c for c in ('timestamp', 'event_ts', 'time') if c in cols]
  if candidates:
    coalesce_expr = f"COALESCE({', '.join(candidates)})"
    conn.execute(f"""
      INSERT OR REPLACE INTO page_views_daily
      SELECT date({coalesce_expr}) AS dt,
             count(*) AS views,
             count(DISTINCT ip) AS uniq_ips
      FROM lake_page_count
      WHERE {coalesce_expr} IS NOT NULL AND date({coalesce_expr}) > ?
      GROUP BY 1
    """, [max_dt])
  elif 'dt' in cols:
    conn.execute("""
      INSERT OR REPLACE INTO page_views_daily
      SELECT dt,
             count(*) AS views,
             count(DISTINCT ip) AS uniq_ips
      FROM lake_page_count
      WHERE dt > ?
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
  for candidate in ('timestamp', 'event_ts'):
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
  # Silver enrichment (user_agent parsing) for page_count via pandas
  p0 = time.time()
  try:
    import pandas as pd
    try:
      cols = [r[0] for r in conn.execute("DESCRIBE lake_page_count").fetchall()]
    except Exception:
      cols = []
    if 'user_agent' in cols:
      df = conn.execute("SELECT * FROM lake_page_count").df()
      if not df.empty:
        try:
          import user_agents  # type: ignore
          parsed = df['user_agent'].fillna('').map(lambda ua: user_agents.parse(ua))
          df['agent_type'] = parsed.map(lambda o: 'bot' if o.is_bot else 'human')
          df['os'] = parsed.map(lambda o: o.os.family or 'unknown')
          df['os_version'] = parsed.map(lambda o: o.os.version_string or '')
          df['browser'] = parsed.map(lambda o: o.browser.family or 'unknown')
          df['browser_version'] = parsed.map(lambda o: o.browser.version_string or '')
          df['device'] = parsed.map(lambda o: o.device.family or 'unknown')
          df['is_mobile'] = parsed.map(lambda o: int(o.is_mobile))
          df['is_tablet'] = parsed.map(lambda o: int(o.is_tablet))
          df['is_pc'] = parsed.map(lambda o: int(o.is_pc))
          df['is_bot'] = parsed.map(lambda o: int(o.is_bot))
        except Exception:
          # Fallback heuristic
            def _fallback_parse(ua: str):
              if not ua: return ('unknown','unknown','', 'unknown','', 'unknown',0,0,0,0)
              u = ua.lower()
              agent_type = 'bot' if any(x in u for x in ['bot','spider','crawl']) else 'human'
              os_name = 'windows' if 'windows' in u else ('android' if 'android' in u else ('linux' if 'linux' in u else ('mac' if 'mac os' in u or 'macintosh' in u else 'unknown')))
              browser = 'chrome' if 'chrome/' in u else ('firefox' if 'firefox/' in u else ('safari' if 'safari/' in u else 'unknown'))
              device = 'mobile' if 'mobile' in u else ('tablet' if 'tablet' in u else ('pc' if os_name in ('windows','linux','mac') else 'unknown'))
              is_mobile = 1 if device=='mobile' else 0
              is_tablet = 1 if device=='tablet' else 0
              is_pc = 1 if device=='pc' else 0
              is_bot = 1 if agent_type=='bot' else 0
              return (agent_type, os_name, '', browser, '', device, is_mobile, is_tablet, is_pc, is_bot)
            parsed = df['user_agent'].fillna('').map(_fallback_parse)
            df['agent_type'] = parsed.map(lambda t: t[0])
            df['os'] = parsed.map(lambda t: t[1])
            df['os_version'] = parsed.map(lambda t: t[2])
            df['browser'] = parsed.map(lambda t: t[3])
            df['browser_version'] = parsed.map(lambda t: t[4])
            df['device'] = parsed.map(lambda t: t[5])
            df['is_mobile'] = parsed.map(lambda t: t[6])
            df['is_tablet'] = parsed.map(lambda t: t[7])
            df['is_pc'] = parsed.map(lambda t: t[8])
            df['is_bot'] = parsed.map(lambda t: t[9])
        # Derive dt
        if 'timestamp' in df.columns:
          df['dt'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.date
        elif 'dt' in df.columns:
          df['dt'] = pd.to_datetime(df['dt'], errors='coerce').dt.date
        else:
          df['dt'] = None
        silver_dir = (ROOT / 'silver' / 'page_count')
        silver_dir.mkdir(parents=True, exist_ok=True)
        out_path = silver_dir / 'page_count_enriched.parquet'
        df.to_parquet(out_path, index=False)
        conn.execute(f"CREATE OR REPLACE VIEW silver_page_count AS SELECT * FROM read_parquet('{out_path}', union_by_name=true)")
      else:
        conn.execute("CREATE OR REPLACE VIEW silver_page_count AS SELECT * FROM lake_page_count WHERE 0=1")
    else:
      conn.execute("CREATE OR REPLACE VIEW silver_page_count AS SELECT * FROM lake_page_count WHERE 0=1")
  except Exception as e:
    print(f"[WARN] Silver enrichment failed: {e}")
  phases['silver_enrich_s'] = round(time.time()-p0, 3)
  # Aggregates
  p0 = time.time(); update_daily_aggregates(conn); phases['aggregates_s'] = round(time.time()-p0, 3)
  # Reports
  p0 = time.time(); run_simple_reports(conn); phases['reports_s'] = round(time.time()-p0, 3)
  # Validation
  p0 = time.time(); validate = validate_simple_pipeline(conn); phases['validation_s'] = round(time.time()-p0, 3)
  # Anomaly detection (does not modify state)
  p0 = time.time(); anomalies = detect_anomalies(conn); phases['anomaly_s'] = round(time.time()-p0, 3)
  total = round(time.time() - t0, 3)
  phases['total_s'] = total
  return {
    'new_files': new_files,
    'latest_page_dt': _scalar(conn, "SELECT max(dt) FROM page_views_daily"),
    'latest_search_dt': _scalar(conn, "SELECT max(dt) FROM searches_daily"),
    'validation': validate,
    'anomalies': {k: len(v.get('anomalies', [])) for k,v in anomalies.get('series', {}).items() if isinstance(v, dict) and 'anomalies' in v},
    'timings': phases,
  }

def fast_load_single_day_csv(conn: duckdb.DuckDBPyConnection, source: str, csv_path: str, dt: date):
  """Fast path: load a single CSV directly into its parquet partition (overwrite) without processed_files checks.

  Useful when source raw data actually arrives as one (or two) consolidated files per day instead of many shards.
  """
  part_dir = LAKE_DIR / source
  part_dir.mkdir(parents=True, exist_ok=True)
  parquet_path = part_dir / f"dt={dt}.parquet"
  # Write to temp then replace to avoid partial write issues
  tmp_path = parquet_path.with_suffix('.parquet.tmp')
  try:
    if tmp_path.exists():
      tmp_path.unlink()
  except Exception:
    pass
  conn.execute(f"COPY (SELECT * FROM read_csv_auto('{csv_path}')) TO '{tmp_path}' (FORMAT PARQUET)")
  # Replace original
  try:
    if parquet_path.exists():
      parquet_path.unlink()
    tmp_path.rename(parquet_path)
  except Exception as e:
    raise RuntimeError(f"Failed replacing parquet partition {parquet_path}: {e}")
  # Minimal processed_files insertion (hashless placeholder using path only)
  fid = hashlib.sha256(f"{csv_path}:{dt}".encode()).hexdigest()
  conn.execute("INSERT OR REPLACE INTO processed_files VALUES (?,?,?,?,?,current_timestamp)", [source, dt, csv_path, fid, conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}')").fetchone()[0]])

def incremental_snapshot_ingest(conn: duckdb.DuckDBPyConnection, source: str, snapshot_path: str, time_col: str = 'timestamp', quality: bool = True) -> dict:
  """Incrementally ingest new rows from a cumulative snapshot CSV into per-day parquet partitions.

  - Uses ingestion_state.last_ts as high-water mark.
  - Filters to rows where time_col > last_ts.
  - Derives dt = date(time_col) and overwrites/creates dt=YYYY-MM-DD.parquet for affected days only.
  - Applies basic quality filters (timestamp not null, query non-empty) when source == 'search_logs'.
  """
  ensure_core_tables(conn)
  last_ts = conn.execute("SELECT last_ts FROM ingestion_state WHERE source=?", [source]).fetchone()
  last_ts_val = last_ts[0] if last_ts else None
  base_read = f"read_csv_auto('{snapshot_path}')"
  # Probe for column existence
  cols = [r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM {base_read}").fetchall()]
  if time_col not in cols:
    raise ValueError(f"Column '{time_col}' not found in snapshot {snapshot_path}")
  quality_clause = ""
  if quality and source == 'search_logs':
    # Expect 'query' column
    if 'query' in cols:
      quality_clause = " AND query IS NOT NULL AND length(trim(query))>0"
  time_filter = f"{time_col} IS NOT NULL" + (f" AND {time_col} > '{last_ts_val}'" if last_ts_val else "")
  # Build projection without creating duplicate dt if file already has dt column
  has_dt_col = 'dt' in cols
  select_cols = '*'
  if has_dt_col:
    # Remove existing dt and recompute to normalize type/date
    non_dt_cols = [c for c in cols if c != 'dt']
    select_cols = ",".join(non_dt_cols)
  new_rows_view = f"SELECT {select_cols}, date({time_col}) AS dt FROM {base_read} WHERE {time_filter}{quality_clause}"
  # Materialize candidate new rows into temp view for reuse
  try:
    conn.execute("CREATE OR REPLACE TEMP VIEW _snapshot_new_rows AS " + new_rows_view)
  except Exception as e:
    raise RuntimeError(f"Failed to stage new rows: {e}")
  new_count = conn.execute("SELECT COUNT(*) FROM _snapshot_new_rows").fetchone()[0]
  if new_count == 0:
    return {'new_rows': 0, 'updated_days': 0, 'last_ts': last_ts_val}
  # Distinct days affected
  days = [r[0] for r in conn.execute("SELECT DISTINCT dt FROM _snapshot_new_rows ORDER BY 1").fetchall()]
  part_dir = LAKE_DIR / source
  part_dir.mkdir(parents=True, exist_ok=True)
  updated = 0
  for dtv in days:
    parquet_path = part_dir / f"dt={dtv}.parquet"
    # Overwrite just that day's partition with union (existing + new for that day) -> DISTINCT optional
    if parquet_path.exists():
      tmp_path = parquet_path.with_suffix('.parquet.tmp')
      try:
        if tmp_path.exists():
          tmp_path.unlink()
      except Exception:
        pass
      # Union existing + new rows into tmp then atomically replace
      conn.execute(f"COPY ((SELECT event_ts, ip, query, dt FROM read_parquet('{parquet_path}')) UNION ALL (SELECT event_ts, ip, query, dt FROM _snapshot_new_rows WHERE dt='{dtv}')) TO '{tmp_path}' (FORMAT PARQUET)")
      try:
        parquet_path.unlink()
        tmp_path.rename(parquet_path)
      except Exception as e:
        raise RuntimeError(f"Failed updating partition {parquet_path}: {e}")
    else:
      conn.execute(f"COPY (SELECT * FROM _snapshot_new_rows WHERE dt='{dtv}') TO '{parquet_path}' (FORMAT PARQUET)")
    updated += 1
  # Advance high-water mark
  max_ts = conn.execute(f"SELECT max({time_col}) FROM _snapshot_new_rows").fetchone()[0]
  conn.execute("INSERT OR REPLACE INTO ingestion_state VALUES (?, ?)", [source, max_ts])
  return {'new_rows': new_count, 'updated_days': updated, 'last_ts': max_ts}


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

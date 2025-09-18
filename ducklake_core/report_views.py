"""Define database views for common reports so they are queryable in DuckDB."""
from __future__ import annotations

import duckdb


def create_report_views(conn: duckdb.DuckDBPyConnection) -> None:
  """Create / incrementally update rollups, export new partitions to parquet, and (re)create analytical views.

  Parquet export rules:
    - Bootstrap: if meta has no last_exported_dt for a rollup OR directory empty -> export all existing dt partitions.
    - Incremental: after inserting new rollup rows, export only dt > last_exported_dt.
    - Meta tracking table 'rollup_meta' stores last_exported_dt per rollup name.
  """
  import pathlib

  # ------------------------------------------------------------------
  # Ensure core rollup tables
  # ------------------------------------------------------------------
  conn.execute("CREATE TABLE IF NOT EXISTS rl_page_views_daily (dt DATE, views BIGINT, uniq_ips BIGINT)")
  conn.execute("CREATE TABLE IF NOT EXISTS rl_page_views_hourly (dt DATE, hour_utc VARCHAR, views BIGINT)")
  conn.execute("CREATE TABLE IF NOT EXISTS rl_searches_daily (dt DATE, query VARCHAR, cnt BIGINT)")
  conn.execute("CREATE TABLE IF NOT EXISTS rl_searches_hourly (dt DATE, hour_utc VARCHAR, searches BIGINT)")

  # Meta table for parquet export progress
  conn.execute("""
    CREATE TABLE IF NOT EXISTS rollup_meta (
      name TEXT PRIMARY KEY,
      last_exported_dt DATE
    )
  """)

  # ------------------------------------------------------------------
  # Incremental daily aggregation (idempotent, append-only)
  # ------------------------------------------------------------------
  prev_page_dt = conn.execute("SELECT COALESCE(max(dt), DATE '1970-01-01') FROM rl_page_views_daily").fetchone()[0]
  prev_search_dt = conn.execute("SELECT COALESCE(max(dt), DATE '1970-01-01') FROM rl_searches_daily").fetchone()[0]

  silver_page_max = conn.execute("SELECT COALESCE(max(dt), DATE '1970-01-01') FROM silver_page_count").fetchone()[0]
  silver_search_max = conn.execute("SELECT COALESCE(max(date(timestamp)), DATE '1970-01-01') FROM silver_search_logs").fetchone()[0]

  if silver_page_max > prev_page_dt:
    conn.execute(
      """
      INSERT INTO rl_page_views_daily
      SELECT dt, count(*) AS views, count(DISTINCT ip) AS uniq_ips
      FROM silver_page_count
      WHERE dt > ?
      GROUP BY 1
      """,
      [prev_page_dt],
    )
  if silver_search_max > prev_search_dt:
    conn.execute(
      """
      INSERT INTO rl_searches_daily
      SELECT date(timestamp) AS dt, lower(query) AS query, count(*) AS cnt
      FROM silver_search_logs
  WHERE timestamp IS NOT NULL AND date(timestamp) > ?
      GROUP BY 1,2
      """,
      [prev_search_dt],
    )

  # ------------------------------------------------------------------
  # Incremental hourly aggregation
  # ------------------------------------------------------------------
  prev_page_hour_ts = conn.execute(
    "SELECT COALESCE(max(CAST(dt || ' ' || LPAD(hour_utc,2,'0') || ':00:00' AS TIMESTAMP)), TIMESTAMP '1970-01-01 00:00:00') FROM rl_page_views_hourly"
  ).fetchone()[0]
  prev_search_hour_ts = conn.execute(
    "SELECT COALESCE(max(CAST(dt || ' ' || LPAD(hour_utc,2,'0') || ':00:00' AS TIMESTAMP)), TIMESTAMP '1970-01-01 00:00:00') FROM rl_searches_hourly"
  ).fetchone()[0]

  # Use raw event timestamps for incremental hourly loads
  max_page_event_ts = conn.execute("SELECT COALESCE(max(COALESCE(ts, timestamp)), TIMESTAMP '1970-01-01 00:00:00') FROM silver_page_count").fetchone()[0]
  max_search_event_ts = conn.execute(
    "SELECT COALESCE(max(timestamp), TIMESTAMP '1970-01-01 00:00:00') FROM silver_search_logs"
  ).fetchone()[0]

  if max_page_event_ts > prev_page_hour_ts:
    conn.execute(
      """
      INSERT INTO rl_page_views_hourly
      SELECT date(COALESCE(ts, timestamp)) AS dt,
             strftime(COALESCE(ts, timestamp), '%H') AS hour_utc,
             count(*) AS views
      FROM silver_page_count
      WHERE COALESCE(ts, timestamp) > ?
      GROUP BY 1,2
      """,
      [prev_page_hour_ts],
    )
  if max_search_event_ts > prev_search_hour_ts:
    conn.execute(
      """
      INSERT INTO rl_searches_hourly
      SELECT date(timestamp) AS dt,
             strftime(timestamp, '%H') AS hour_utc,
             count(*) AS searches
      FROM silver_search_logs
      WHERE timestamp > ?
      GROUP BY 1,2
      """,
      [prev_search_hour_ts],
    )

  # ------------------------------------------------------------------
  # Parquet export (daily rollups only for now)
  # ------------------------------------------------------------------
  base_dir = pathlib.Path(__file__).resolve().parent.parent / "rollups"
  pv_dir = base_dir / "page_views_daily"; pv_dir.mkdir(parents=True, exist_ok=True)
  sr_dir = base_dir / "searches_daily"; sr_dir.mkdir(parents=True, exist_ok=True)

  # Read/export progress from meta
  import datetime as _dt

  SENTINEL = _dt.date(1970, 1, 1)

  def _get_last_exported(name: str):
    row = conn.execute("SELECT last_exported_dt FROM rollup_meta WHERE name=?", [name]).fetchone()
    return row[0] if row and row[0] is not None else SENTINEL

  def _update_last_exported(name: str, dt_val):
    conn.execute(
      "INSERT INTO rollup_meta (name, last_exported_dt) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET last_exported_dt=excluded.last_exported_dt",
      [name, dt_val],
    )

  def _export_daily(table: str, name: str, target_dir: pathlib.Path):
    last_dt = _get_last_exported(name)
    existing_files = list(target_dir.glob("dt=*.parquet"))
    if not existing_files and last_dt == SENTINEL:
      # Bootstrap export all non-null dt
      rows = conn.execute(f"SELECT DISTINCT dt FROM {table} WHERE dt IS NOT NULL ORDER BY 1").fetchall()
    else:
      rows = conn.execute(f"SELECT DISTINCT dt FROM {table} WHERE dt > ? AND dt IS NOT NULL ORDER BY 1", [last_dt]).fetchall()
    if not rows:
      return 0
    for (dt_val,) in rows:
      outfile = target_dir / f"dt={dt_val}.parquet"
      # DuckDB COPY doesn't accept file path as a parameter; inline path safely.
      conn.execute(
        f"COPY (SELECT * FROM {table} WHERE dt = '{dt_val}' ) TO '{outfile}' (FORMAT PARQUET)"
      )
    _update_last_exported(name, rows[-1][0])
    return len(rows)

  _export_daily("rl_page_views_daily", "page_views_daily", pv_dir)
  _export_daily("rl_searches_daily", "searches_daily", sr_dir)

  # ------------------------------------------------------------------
  # Parquet-backed views (fallback if no files yet)
  # ------------------------------------------------------------------
  def _create_or_fallback(view_name: str, path_glob: str, table_fallback: str):
    try:
      # If no files match, DuckDB raises; catch and fallback
      conn.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{path_glob}')"
      )
    except Exception:
      conn.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_fallback}"
      )

  _create_or_fallback("vw_rl_page_views_daily", f"{pv_dir}/dt=*.parquet", "rl_page_views_daily")
  _create_or_fallback("vw_rl_searches_daily", f"{sr_dir}/dt=*.parquet", "rl_searches_daily")

  # ------------------------------------------------------------------
  # Indexes (lightweight)
  # ------------------------------------------------------------------
  for stmt in [
    "CREATE INDEX IF NOT EXISTS idx_rl_pvd_dt ON rl_page_views_daily(dt)",
    "CREATE INDEX IF NOT EXISTS idx_rl_svd_dt_query ON rl_searches_daily(dt, query)",
    "CREATE INDEX IF NOT EXISTS idx_rl_pvh_dt_hour ON rl_page_views_hourly(dt, hour_utc)",
    "CREATE INDEX IF NOT EXISTS idx_rl_svh_dt_hour ON rl_searches_hourly(dt, hour_utc)",
  ]:
    try:
      conn.execute(stmt)
    except Exception:
      pass

  # ------------------------------------------------------------------
  # Analytical views (single definitions)
  # ------------------------------------------------------------------
  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_searches_today AS
    SELECT date(timestamp) AS dt, timestamp, ip, query
    FROM silver_search_logs
    WHERE date(timestamp) = current_date
    ORDER BY timestamp
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_searches_latest_day AS
    WITH latest_day AS (SELECT max(date(timestamp)) AS dt FROM silver_search_logs)
    SELECT date(s.timestamp) AS dt, s.timestamp, s.ip, s.query
    FROM silver_search_logs s, latest_day ld
    WHERE date(s.timestamp) = ld.dt
    ORDER BY s.timestamp
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_top_queries_all_time AS
    SELECT query, cnt FROM (
      SELECT lower(query) AS query, count(*) AS cnt
      FROM silver_search_logs
      GROUP BY 1
      ORDER BY cnt DESC
      LIMIT 100
    )
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_top_queries_30d AS
    SELECT lower(query) AS query, count(*) AS cnt
    FROM silver_search_logs
    WHERE timestamp >= now() - INTERVAL 30 DAY
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 100
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_trending_queries_1d_vs_7d AS
    WITH recent AS (
      SELECT lower(query) AS q, count(*) AS c_recent
      FROM silver_search_logs
      WHERE timestamp >= now() - INTERVAL 1 DAY
      GROUP BY 1
    ), baseline AS (
      SELECT lower(query) AS q, count(*)/7.0 AS c_daily_baseline
      FROM silver_search_logs
      WHERE timestamp >= now() - INTERVAL 8 DAY AND timestamp < now() - INTERVAL 1 DAY
      GROUP BY 1
    )
    SELECT r.q AS query,
           r.c_recent,
           b.c_daily_baseline,
           (r.c_recent / NULLIF(b.c_daily_baseline, 0)) AS lift
    FROM recent r
    LEFT JOIN baseline b ON b.q = r.q
    ORDER BY lift DESC NULLS LAST, c_recent DESC
    LIMIT 100
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_busiest_hours_utc AS
    SELECT hour_utc, sum(views) AS cnt
    FROM rl_page_views_hourly
    GROUP BY 1
    ORDER BY 1
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_busiest_days_of_week AS
    SELECT strftime(dt, '%w') AS dow_num,
           CASE strftime(dt, '%w')
             WHEN '0' THEN 'Sun'
             WHEN '1' THEN 'Mon'
             WHEN '2' THEN 'Tue'
             WHEN '3' THEN 'Wed'
             WHEN '4' THEN 'Thu'
             WHEN '5' THEN 'Fri'
             WHEN '6' THEN 'Sat'
           END AS dow,
           sum(views) AS visits
  FROM vw_rl_page_views_daily
    GROUP BY 1,2
    ORDER BY 1
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_popular_ip_prefix24 AS
    SELECT regexp_replace(ip, '\\.?[0-9]+$', '.0') AS ip_prefix24,
           count(*) AS cnt
    FROM silver_page_count
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT 100
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_visits_pages_daily AS
    SELECT dt, sum(views) AS cnt
    FROM rl_page_views_daily
    GROUP BY 1
    ORDER BY 1
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_visits_pages_weekly AS
    WITH daily AS (
  SELECT dt, sum(views) AS cnt FROM vw_rl_page_views_daily GROUP BY 1
    )
    SELECT strftime(dt, '%G-%V') AS iso_week, sum(cnt) AS week_cnt
    FROM daily
    GROUP BY 1
    ORDER BY 1
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_visits_pages_monthly AS
    SELECT strftime(dt, '%Y-%m') AS yyyymm, sum(views) AS cnt
  FROM vw_rl_page_views_daily
    GROUP BY 1
    ORDER BY 1
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_searches_daily AS
    SELECT dt, query, cnt
  FROM vw_rl_searches_daily
    ORDER BY dt, cnt DESC
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_visits_pages_wow AS
    WITH daily AS (
  SELECT dt, sum(views) AS cnt FROM vw_rl_page_views_daily GROUP BY 1
    ), wk AS (
      SELECT strftime(dt, '%G-%V') AS iso_week, sum(cnt) AS week_cnt FROM daily GROUP BY 1
    )
    SELECT w1.iso_week, w1.week_cnt,
           lag(w1.week_cnt) OVER (ORDER BY w1.iso_week) AS prev_week,
           (w1.week_cnt - lag(w1.week_cnt) OVER (ORDER BY w1.iso_week)) AS delta,
           (w1.week_cnt / NULLIF(lag(w1.week_cnt) OVER (ORDER BY w1.iso_week), 0) - 1) AS pct_change
    FROM wk w1
    ORDER BY w1.iso_week DESC
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_engagement_most AS
    WITH stats AS (
      SELECT url, count(*) AS views, count(DISTINCT ip) AS uniq_visitors
      FROM silver_page_count GROUP BY 1
    )
    SELECT * FROM stats WHERE views >= 5 ORDER BY views DESC LIMIT 100
    """
  )

  conn.execute(
    """
    CREATE OR REPLACE VIEW vw_engagement_least AS
    WITH stats AS (
      SELECT url, count(*) AS views, count(DISTINCT ip) AS uniq_visitors
      FROM silver_page_count GROUP BY 1
    )
    SELECT * FROM stats WHERE views >= 5 ORDER BY views ASC, uniq_visitors ASC LIMIT 100
    """
  )

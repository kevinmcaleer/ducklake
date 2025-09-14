"""Define database views for common reports so they are queryable in DuckDB."""
from __future__ import annotations

import duckdb


def create_report_views(conn: duckdb.DuckDBPyConnection) -> None:
    # Searches made today (dynamic by current_date)
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_searches_today AS
        SELECT dt, timestamp, ip, query
        FROM silver_search_logs
        WHERE dt = current_date
        ORDER BY timestamp
        """
    )

    # Top queries (all time)
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_top_queries_all_time AS
        SELECT lower(query) AS query, count(*) AS cnt
        FROM silver_search_logs
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 100
        """
    )

    # Top queries (last 30 days)
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

    # Trending now vs 7-day baseline
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_trending_queries_1d_vs_7d AS
        WITH recent AS (
          SELECT lower(query) AS q, count(*) AS c_recent
          FROM silver_search_logs
          WHERE timestamp >= now() - INTERVAL 1 DAY
          GROUP BY 1
        ),
        baseline AS (
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

    # Busiest times (page views)
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_busiest_hours_utc AS
        SELECT strftime(ts, '%H') AS hour_utc, count(*) AS cnt
        FROM silver_page_count
        GROUP BY 1
        ORDER BY 1
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_busiest_days_of_week AS
        SELECT strftime(ts, '%w') AS dow_num,  -- 0=Sun..6=Sat
               strftime(ts, '%a') AS dow,
               count(*) AS cnt
        FROM silver_page_count
        GROUP BY 1,2
        ORDER BY 1
        """
    )

    # Popular locations (/24 prefix proxy)
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

    # Visits (pages) over periods
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_visits_pages_daily AS
        SELECT dt, count(*) AS cnt
        FROM silver_page_count
        GROUP BY 1
        ORDER BY 1
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_visits_pages_weekly AS
        WITH daily AS (
          SELECT dt, count(*) AS cnt
          FROM silver_page_count
          GROUP BY 1
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
        SELECT strftime(dt, '%Y-%m') AS yyyymm, count(*) AS cnt
        FROM silver_page_count
        GROUP BY 1
        ORDER BY 1
        """
    )

    # Searches per day by query
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_searches_daily AS
        SELECT dt, lower(query) AS query, count(*) AS cnt
        FROM silver_search_logs
        GROUP BY 1, 2
        ORDER BY dt, cnt DESC
        """
    )

    # Week-over-week trend (pages)
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_visits_pages_wow AS
        WITH daily AS (
          SELECT dt, count(*) AS cnt
          FROM silver_page_count
          GROUP BY 1
        ),
        wk AS (
          SELECT strftime(dt, '%G-%V') AS iso_week, sum(cnt) AS week_cnt
          FROM daily
          GROUP BY 1
        )
        SELECT w1.iso_week, w1.week_cnt,
               lag(w1.week_cnt) OVER (ORDER BY w1.iso_week) AS prev_week,
               (w1.week_cnt - lag(w1.week_cnt) OVER (ORDER BY w1.iso_week)) AS delta,
               (w1.week_cnt / NULLIF(lag(w1.week_cnt) OVER (ORDER BY w1.iso_week), 0) - 1) AS pct_change
        FROM wk w1
        ORDER BY w1.iso_week DESC
        """
    )

    # Engagement (most/least)
    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_engagement_most AS
        WITH stats AS (
          SELECT url,
                 count(*) AS views,
                 count(DISTINCT ip) AS uniq_visitors
          FROM silver_page_count
          GROUP BY 1
        )
        SELECT *
        FROM stats
        WHERE views >= 5
        ORDER BY views DESC
        LIMIT 100
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE VIEW vw_engagement_least AS
        WITH stats AS (
          SELECT url,
                 count(*) AS views,
                 count(DISTINCT ip) AS uniq_visitors
          FROM silver_page_count
          GROUP BY 1
        )
        SELECT *
        FROM stats
        WHERE views >= 5
        ORDER BY views ASC, uniq_visitors ASC
        LIMIT 100
        """
    )

-- CSV report exports for common questions (writes into ./reports/)
-- Ensure the ./reports directory exists before running this script.
-- Usage:
-- duckdb duckdb/lake.duckdb -c ".read reports.sql"

-- 1) Most popular search keywords and phrases
COPY (
  SELECT title AS query, total_value AS cnt
  FROM gold_search_logs_all_time
  ORDER BY cnt DESC
  LIMIT 100
) TO 'reports/top_queries_all_time.csv' (FORMAT CSV, HEADER TRUE);

COPY (
  SELECT lower(query) AS query, count(*) AS cnt
  FROM silver_search_logs
  WHERE ts >= now() - INTERVAL 30 DAY
  GROUP BY 1
  ORDER BY cnt DESC
  LIMIT 100
) TO 'reports/top_queries_30d.csv' (FORMAT CSV, HEADER TRUE);

-- 2) Trending now vs 7-day baseline
COPY (
WITH recent AS (
  SELECT lower(query) AS q, count(*) AS c_recent
  FROM silver_search_logs
  WHERE ts >= now() - INTERVAL 1 DAY
  GROUP BY 1
),
baseline AS (
  SELECT lower(query) AS q, count(*)/7.0 AS c_daily_baseline
  FROM silver_search_logs
  WHERE ts >= now() - INTERVAL 8 DAY AND ts < now() - INTERVAL 1 DAY
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
) TO 'reports/trending_queries_1d_vs_7d.csv' (FORMAT CSV, HEADER TRUE);

-- 3) Busiest times
COPY (
  SELECT strftime(ts, '%H') AS hour_utc, count(*) AS cnt
  FROM silver_page_count
  GROUP BY 1
  ORDER BY 1
) TO 'reports/busiest_hours_utc.csv' (FORMAT CSV, HEADER TRUE);

COPY (
  SELECT strftime(ts, '%w') AS dow_num,  -- 0=Sun..6=Sat
         strftime(ts, '%a') AS dow,
         count(*) AS cnt
  FROM silver_page_count
  GROUP BY 1,2
  ORDER BY 1
) TO 'reports/busiest_days_of_week.csv' (FORMAT CSV, HEADER TRUE);

-- 4) Popular locations (IP /24 prefix as proxy)
COPY (
  SELECT regexp_replace(ip, '\\.?[0-9]+$', '.0') AS ip_prefix24,
         count(*) AS cnt
  FROM silver_page_count
  GROUP BY 1
  ORDER BY cnt DESC
  LIMIT 100
) TO 'reports/popular_ip_prefix24.csv' (FORMAT CSV, HEADER TRUE);

-- 5) Visits per day/week/month (pages)
COPY (
  SELECT dt, count(*) AS cnt
  FROM silver_page_count
  GROUP BY 1
  ORDER BY 1
) TO 'reports/visits_pages_daily.csv' (FORMAT CSV, HEADER TRUE);

COPY (
  SELECT strftime(dt, '%G-%V') AS iso_week, count(*) AS cnt
  FROM silver_page_count
  GROUP BY 1
  ORDER BY 1
) TO 'reports/visits_pages_weekly.csv' (FORMAT CSV, HEADER TRUE);

COPY (
  SELECT strftime(dt, '%Y-%m') AS yyyymm, count(*) AS cnt
  FROM silver_page_count
  GROUP BY 1
  ORDER BY 1
) TO 'reports/visits_pages_monthly.csv' (FORMAT CSV, HEADER TRUE);

-- (Searches) per day
COPY (
  SELECT dt, count(*) AS cnt
  FROM silver_search_logs
  GROUP BY 1
  ORDER BY 1
) TO 'reports/searches_daily.csv' (FORMAT CSV, HEADER TRUE);

-- 6) Week-over-week trend (pages)
COPY (
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
) TO 'reports/visits_pages_wow.csv' (FORMAT CSV, HEADER TRUE);

-- 7) Engagement (most / least)
COPY (
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
) TO 'reports/engagement_most.csv' (FORMAT CSV, HEADER TRUE);

COPY (
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
) TO 'reports/engagement_least.csv' (FORMAT CSV, HEADER TRUE);

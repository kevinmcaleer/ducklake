-- DuckDB analysis queries for the questions in questions.md
-- Assumes views created by intake.py:
--   silver_page_count, silver_search_logs
--   gold_page_count_daily, gold_page_count_all_time
--   gold_search_logs_daily, gold_search_logs_all_time

-- 1) Most popular search keywords and phrases (all-time)
-- Top queries (gold)
SELECT title AS query, total_value AS cnt
FROM gold_search_logs_all_time
ORDER BY cnt DESC
LIMIT 50;

-- Top queries in the last 30 days (silver)
SELECT lower(query) AS query, count(*) AS cnt
FROM silver_search_logs
WHERE ts >= now() - INTERVAL 30 DAY
GROUP BY 1
ORDER BY cnt DESC
LIMIT 50;

-- Optional: token-level keywords (simple whitespace split, minimal stopwording)
WITH toks AS (
  SELECT u.tok
  FROM silver_search_logs s,
       UNNEST(string_split(lower(s.query), ' ')) AS u(tok)
  WHERE s.ts >= now() - INTERVAL 30 DAY
)
SELECT tok, count(*) AS cnt
FROM toks
WHERE length(tok) >= 3
  AND tok NOT IN ('the','and','for','you','are','with','from','this','that','have','has','was','were','your','our')
GROUP BY 1
ORDER BY cnt DESC
LIMIT 50;

-- 2) What is trending now (vs. 7-day baseline)
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
LIMIT 50;

-- 3) Busiest times, days of week and times of the day (page views)
-- By hour of day (UTC)
SELECT strftime(ts, '%H') AS hour_utc, count(*) AS cnt
FROM silver_page_count
GROUP BY 1
ORDER BY 1;

-- By day of week
SELECT strftime(ts, '%w') AS dow_num,  -- 0=Sun..6=Sat
       strftime(ts, '%a') AS dow,
       count(*) AS cnt
FROM silver_page_count
GROUP BY 1, 2
ORDER BY 1;

-- 4) Most popular locations (proxy by IP prefix until GeoIP is added)
SELECT regexp_replace(ip, '\\.\\d+$', '.0') AS ip_prefix24,
       count(*) AS cnt
FROM silver_page_count
GROUP BY 1
ORDER BY cnt DESC
LIMIT 50;

-- 5) How many visits per day, per week, per month (pages)
-- Per day
SELECT dt, count(*) AS cnt
FROM silver_page_count
GROUP BY 1
ORDER BY 1;

-- Per week (ISO week)
SELECT strftime(dt, '%G-%V') AS iso_week, count(*) AS cnt
FROM silver_page_count
GROUP BY 1
ORDER BY 1;

-- Per month (YYYY-MM)
SELECT strftime(dt, '%Y-%m') AS yyyymm, count(*) AS cnt
FROM silver_page_count
GROUP BY 1
ORDER BY 1;

-- (Searches) Per day
SELECT dt, count(*) AS cnt
FROM silver_search_logs
GROUP BY 1
ORDER BY 1;

-- 6) Are visits increasing or decreasing (WoW, pages)
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
ORDER BY w1.iso_week DESC;

-- 7) Content engagement (most / least engaging by views and unique IPs)
-- Most engaging (min threshold to reduce noise)
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
LIMIT 50;

-- Least engaging (among pages with some traffic)
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
LIMIT 50;

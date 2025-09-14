COPY (
  SELECT DISTINCT
    dt,
    timestamp,
    ip,
    query
  FROM silver_search_logs
  WHERE dt = current_date
  ORDER BY timestamp
) TO 'reports/searches_today.csv' (HEADER, DELIMITER ',');

COPY (
  WITH daily_visits AS (
    SELECT dt, count(*) AS visits
    FROM silver_page_count
    GROUP BY dt
  ),
  daily_searches AS (
    SELECT dt, count(*) AS searches
    FROM silver_search_logs
    GROUP BY dt
  ),
  all_days AS (
    SELECT dt FROM daily_visits
    UNION
    SELECT dt FROM daily_searches
  ),
  joined AS (
    SELECT d.dt,
           COALESCE(v.visits, 0) AS visits,
           COALESCE(s.searches, 0) AS searches
    FROM all_days d
    LEFT JOIN daily_visits v ON d.dt = v.dt
    LEFT JOIN daily_searches s ON d.dt = s.dt
  )
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
         sum(visits) AS visits,
         sum(searches) AS searches
  FROM joined
  GROUP BY dow_num, dow
  ORDER BY dow_num
) TO 'reports/busiest_days_of_week.csv' (FORMAT CSV, HEADER TRUE);
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
  WITH hours AS (
    SELECT * FROM (VALUES
      ('00'),('01'),('02'),('03'),('04'),('05'),('06'),('07'),('08'),('09'),('10'),('11'),
      ('12'),('13'),('14'),('15'),('16'),('17'),('18'),('19'),('20'),('21'),('22'),('23')
    ) AS t(hour_utc)
  ),
  visits AS (
    SELECT strftime(COALESCE(ts, timestamp), '%H') AS hour_utc, count(*) AS visits
    FROM silver_page_count
    GROUP BY 1
  ),
  searches AS (
    SELECT strftime(COALESCE(ts, timestamp), '%H') AS hour_utc, count(*) AS searches
    FROM silver_search_logs
    GROUP BY 1
  )
  SELECT h.hour_utc, COALESCE(v.visits, 0) AS visits, COALESCE(s.searches, 0) AS searches
  FROM hours h
  LEFT JOIN visits v ON h.hour_utc = v.hour_utc
  LEFT JOIN searches s ON h.hour_utc = s.hour_utc
  ORDER BY h.hour_utc
) TO 'reports/busiest_hours_utc.csv' (FORMAT CSV, HEADER TRUE);
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
-- Searches per day by query (so you can see what the searches are)
COPY (
  SELECT dt, lower(query) AS query, count(*) AS cnt
  FROM silver_search_logs
  GROUP BY 1, 2
  ORDER BY dt, cnt DESC
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

COPY (
  SELECT DISTINCT
    dt,
    timestamp,
    ip,
    query
  FROM silver_search_logs
  WHERE dt = current_date
  ORDER BY timestamp
) TO 'reports/searches_today.csv' (HEADER, DELIMITER ',');
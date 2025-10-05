-- Insert new page view aggregates from lake_page_count using dt column
-- Fallback when no timestamp columns are available
INSERT OR REPLACE INTO page_views_daily
SELECT dt,
       count(*) AS views,
       count(DISTINCT ip) AS uniq_ips
FROM lake_page_count
WHERE dt > ?
GROUP BY 1;
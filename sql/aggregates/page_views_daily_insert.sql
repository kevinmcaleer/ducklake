-- Insert new page view aggregates from lake_page_count
-- Uses dynamic timestamp column detection via {coalesce_expr} parameter
INSERT OR REPLACE INTO page_views_daily
SELECT date({coalesce_expr}) AS dt,
       count(*) AS views,
       count(DISTINCT ip) AS uniq_ips
FROM lake_page_count
WHERE {coalesce_expr} IS NOT NULL AND date({coalesce_expr}) > ?
GROUP BY 1;
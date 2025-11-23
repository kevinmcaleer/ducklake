-- Insert new search aggregates from lake_search_logs using dt column
-- Fallback when no timestamp columns are available
-- Includes quality filters for query normalization
-- Counts DISTINCT users (by IP) to prevent gaming by repeated searches
INSERT OR REPLACE INTO searches_daily
SELECT dt,
       lower(trim(regexp_replace(query, '\s+', ' '))) AS query,
       count(DISTINCT ip) AS cnt
FROM lake_search_logs
WHERE dt > ?
  AND query IS NOT NULL
  AND length(trim(query)) > 0
  AND lower(trim(query)) <> 'null'
  AND length(lower(trim(regexp_replace(query, '\s+', ' ')))) > 1
GROUP BY 1, 2;
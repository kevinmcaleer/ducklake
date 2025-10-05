-- Insert new search aggregates from lake_search_logs
-- Uses dynamic timestamp column via {time_col} parameter
-- Includes quality filters for query normalization
INSERT OR REPLACE INTO searches_daily
SELECT date({time_col}) AS dt,
       lower(trim(regexp_replace(query, '\s+', ' '))) AS query,
       count(*) AS cnt
FROM lake_search_logs
WHERE {time_col} IS NOT NULL
  AND query IS NOT NULL
  AND length(trim(query)) > 0
  AND lower(trim(query)) <> 'null'
  AND length(lower(trim(regexp_replace(query, '\s+', ' ')))) > 1
  AND date({time_col}) > ?
GROUP BY 1, 2;
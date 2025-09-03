-- Aggregate search logs by country using GeoIP blocks and locations (IPv4 only)
WITH search_logs_int AS (
  SELECT
    *,
    (
      CAST(split_part(ip, '.', 1) AS BIGINT) * 16777216 +
      CAST(split_part(ip, '.', 2) AS BIGINT) * 65536 +
      CAST(split_part(ip, '.', 3) AS BIGINT) * 256 +
      CAST(split_part(ip, '.', 4) AS BIGINT)
    ) AS ip_int
  FROM silver_search_logs
  WHERE ip ~ '^[0-9.]+$'  -- Only IPv4 for now
),

geo_blocks AS (
  SELECT
    network,
    geoname_id,
    registered_country_geoname_id,
    represented_country_geoname_id,
    regexp_replace(network, '/.*', '') AS net_start,
    CAST(split_part(network, '/', 2) AS INTEGER) AS prefix_len
  FROM silver_geoip_blocks
  WHERE network IS NOT NULL
),

geo_blocks_expanded AS (
  SELECT
    *,
    (
      CAST(split_part(net_start, '.', 1) AS BIGINT) * 16777216 +
      CAST(split_part(net_start, '.', 2) AS BIGINT) * 65536 +
      CAST(split_part(net_start, '.', 3) AS BIGINT) * 256 +
      CAST(split_part(net_start, '.', 4) AS BIGINT)
    ) AS net_start_int,
    (
      CAST(split_part(net_start, '.', 1) AS BIGINT) * 16777216 +
      CAST(split_part(net_start, '.', 2) AS BIGINT) * 65536 +
      CAST(split_part(net_start, '.', 3) AS BIGINT) * 256 +
      CAST(split_part(net_start, '.', 4) AS BIGINT) + (1::BIGINT << (32 - prefix_len)) - 1
    ) AS net_end_int
  FROM geo_blocks
),

logs_with_country AS (
  SELECT
    s.*,
    g.registered_country_geoname_id
  FROM search_logs_int s
  LEFT JOIN geo_blocks_expanded g
    ON s.ip_int BETWEEN g.net_start_int AND g.net_end_int
)

SELECT
  l.country_name,
  COUNT(*) AS search_count
FROM logs_with_country s
LEFT JOIN silver_geoip_locations l
  ON s.registered_country_geoname_id = l.geoname_id
WHERE l.country_name IS NOT NULL
GROUP BY l.country_name
ORDER BY search_count DESC
LIMIT 20;

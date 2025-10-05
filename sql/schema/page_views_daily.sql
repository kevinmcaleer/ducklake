-- Daily aggregated page view metrics
CREATE TABLE IF NOT EXISTS page_views_daily (
    dt DATE PRIMARY KEY,
    views BIGINT,
    uniq_ips BIGINT
);
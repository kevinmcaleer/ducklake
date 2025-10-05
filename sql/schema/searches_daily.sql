-- Daily aggregated search query metrics
-- Allow NULL query temporarily (filter out during aggregation) by not enforcing NOT NULL constraint
CREATE TABLE IF NOT EXISTS searches_daily (
    dt DATE,
    query TEXT,
    cnt BIGINT,
    PRIMARY KEY (dt, query)
);
-- Performance indexes for key tables
-- DuckDB will treat these as projections in newer versions
CREATE INDEX IF NOT EXISTS idx_searches_daily_dt ON searches_daily(dt);
CREATE INDEX IF NOT EXISTS idx_searches_daily_query ON searches_daily(query);
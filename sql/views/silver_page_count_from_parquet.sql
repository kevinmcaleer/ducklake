-- Create silver_page_count view from enriched parquet file
-- Used when user agent enrichment has been completed
CREATE OR REPLACE VIEW silver_page_count AS
SELECT * FROM read_parquet('{parquet_path}', union_by_name=true);
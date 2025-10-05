-- Create empty silver_page_count view with user agent enrichment columns
-- Used when no data is available or user_agent column is missing
CREATE OR REPLACE VIEW silver_page_count AS
SELECT *,
    CAST(NULL AS VARCHAR) AS agent_type,
    CAST(NULL AS VARCHAR) AS os,
    CAST(NULL AS VARCHAR) AS os_version,
    CAST(NULL AS VARCHAR) AS browser,
    CAST(NULL AS VARCHAR) AS browser_version,
    CAST(NULL AS VARCHAR) AS device,
    CAST(NULL AS INTEGER) AS is_mobile,
    CAST(NULL AS INTEGER) AS is_tablet,
    CAST(NULL AS INTEGER) AS is_pc,
    CAST(NULL AS INTEGER) AS is_bot
FROM lake_page_count WHERE 0=1;
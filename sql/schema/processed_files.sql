-- Table to track processed raw files to avoid duplicate ingestion
CREATE TABLE IF NOT EXISTS processed_files (
    source TEXT,
    dt DATE,
    path TEXT,
    file_id TEXT PRIMARY KEY,
    rows BIGINT,
    ingested_at TIMESTAMP
);
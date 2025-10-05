-- High-water mark per source for snapshot-style incremental ingestion
CREATE TABLE IF NOT EXISTS ingestion_state (
    source TEXT PRIMARY KEY,
    last_ts TIMESTAMP
);
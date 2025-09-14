import pathlib
import duckdb

from ducklake_core.bronze import ingest_bronze
from ducklake_core.silver import build_silver_from_manifest


def setup_db(conn):
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS manifest (
      source TEXT,
      run_ts TIMESTAMP,
      dt DATE,
      path TEXT,
      bytes BIGINT,
      content_sha256 TEXT,
      rows BIGINT,
      format TEXT,
      meta JSON
    )
    """
    )


def test_log_parsing_simple_to_silver(tmp_path):
    conn = duckdb.connect(str(tmp_path / "t.duckdb"))
    setup_db(conn)
    lake = tmp_path

    # Create a tiny log file matching the simple parser format
    log = tmp_path / "search-logs.log"
    log.write_text(
        "INFO:root:2024-01-07T13:13:28.736047 - IP: 10.0.0.5 - Query: simple search\n",
        encoding="utf-8",
    )

    # Ingest as bronze (format=log)
    ingest_bronze(conn, lake, "search_logs", str(log), "log")

    # Build silver using simple parse method
    cfg = {
        "expect_schema": {
            "timestamp": "datetime",
            "ip": "string",
            "query": "string",
        },
        "parse": {
            "method": "simple",
            "ts_format": "%Y-%m-%dT%H:%M:%S.%f",
        },
    }

    build_silver_from_manifest(conn, lake, "search_logs", cfg)

    # Verify silver view exists and has expected columns
    rows = conn.execute("SELECT timestamp, ip, query FROM silver_search_logs").fetchall()
    assert rows and rows[0][1] == "10.0.0.5" and rows[0][2] == "simple search"

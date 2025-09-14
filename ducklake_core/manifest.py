"""
Manifest management utilities for DuckLake.
"""
import duckdb
from typing import List, Tuple, Optional

def get_manifest_rows(conn: duckdb.DuckDBPyConnection, source: str) -> List[Tuple]:
    """Fetch manifest rows for a given source."""
    return conn.execute(
        """
        SELECT path, format, dt FROM manifest WHERE source = ? ORDER BY run_ts
        """,
        [source],
    ).fetchall()

def print_manifest(conn: duckdb.DuckDBPyConnection):
    rows = conn.execute("SELECT source, dt, path, format, rows FROM manifest").fetchall()
    print(f"MANIFEST: {rows}")

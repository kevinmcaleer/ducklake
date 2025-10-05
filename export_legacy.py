#!/usr/bin/env python3
"""Export reports using DuckDB 1.3.1 for Metabase compatibility."""
import sys
import pathlib

# This script should be run with DuckDB 1.3.1
import duckdb

def export_with_legacy_format(source_db: str, target_db: str):
    """Export database using DuckDB 1.3.1 format."""
    source_path = pathlib.Path(source_db)
    target_path = pathlib.Path(target_db)

    if not source_path.exists():
        print(f"Error: Source database not found: {source_path}")
        sys.exit(1)

    # Remove existing target
    if target_path.exists():
        target_path.unlink()

    # Create new database with 1.3.1
    target_conn = duckdb.connect(str(target_path))

    try:
        target_conn.execute(f"ATTACH '{source_path}' AS src (READ_ONLY)")

        # Get all tables
        tables = target_conn.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'main'
            AND table_catalog = 'src'
            AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
        """).fetchall()

        print(f"Exporting {len(tables)} objects...")

        for table_name, table_type in tables:
            target_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            target_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM src.{table_name}")
            print(f"  ✓ {table_name}")

        target_conn.execute("DETACH src")
        target_conn.close()

        print(f"\nExported {len(tables)} objects to {target_path}")
        print(f"DuckDB version: {duckdb.__version__}")

    except Exception as e:
        target_conn.close()
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    export_with_legacy_format("contentlake.ducklake", "reports.duckdb")

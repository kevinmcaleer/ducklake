#!/usr/bin/env python3
"""Export reports with explicit schema handling for Metabase."""
import sys
import pathlib
import duckdb

def export_for_metabase(source_db: str, target_db: str):
    """Export database with schema workarounds for Metabase."""
    source_path = pathlib.Path(source_db)
    target_path = pathlib.Path(target_db)

    if not source_path.exists():
        print(f"Error: Source database not found: {source_path}")
        sys.exit(1)

    # Remove existing target
    if target_path.exists():
        target_path.unlink()

    # Create new database
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

        print(f"Exporting {len(tables)} objects to main schema...")

        # Create tables in main schema (default)
        for table_name, table_type in tables:
            target_conn.execute(f"CREATE TABLE main.{table_name} AS SELECT * FROM src.{table_name}")
            print(f"  ✓ {table_name}")

        target_conn.execute("DETACH src")

        # Verify tables exist
        verify = target_conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main'").fetchone()[0]
        print(f"\nVerification: {verify} tables in main schema")

        # List all schemas
        schemas = target_conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        print(f"Available schemas: {[s[0] for s in schemas]}")

        target_conn.close()

        print(f"\nExported to {target_path}")
        print(f"DuckDB version: {duckdb.__version__}")

    except Exception as e:
        target_conn.close()
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    export_for_metabase("contentlake.ducklake", "reports.duckdb")

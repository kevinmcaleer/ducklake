"""Export reports and views from contentlake to a separate reporting database for Metabase.

NOTE: This export requires DuckDB 1.3.1 for Metabase compatibility.
Run with: source /tmp/duckdb131_env/bin/activate && python3 run_refresh.py --db contentlake.ducklake export
"""
from __future__ import annotations
import duckdb
import pathlib
import sys
from typing import Optional

ROOT = pathlib.Path(__file__).resolve().parent.parent


def export_to_reports_db(
    source_db_path: str | pathlib.Path,
    target_db_path: str | pathlib.Path = "reports.duckdb",
    tables_only: bool = False
) -> dict:
    """Export all tables and views from source database to target reporting database.

    IMPORTANT: Must be run with DuckDB 1.3.1 for Metabase compatibility.
    The export creates a DuckDB 1.3.1 format database that Metabase can read.

    Args:
        source_db_path: Path to source DuckDB database (contentlake.ducklake)
        target_db_path: Path to target reporting database (default: reports.duckdb)
        tables_only: If True, only export base tables, not views

    Returns:
        Dictionary with export statistics
    """
    # Check DuckDB version
    if duckdb.__version__ != "1.3.1":
        raise RuntimeError(
            f"Export requires DuckDB 1.3.1 for Metabase compatibility. "
            f"Current version: {duckdb.__version__}\n"
            f"Run with: source /tmp/duckdb131_env/bin/activate && python3 run_refresh.py --db contentlake.ducklake export"
        )

    source_db_path = pathlib.Path(source_db_path)
    target_db_path = pathlib.Path(target_db_path)

    if not source_db_path.exists():
        raise FileNotFoundError(f"Source database not found: {source_db_path}")

    # Delete existing target database to start fresh
    if target_db_path.exists():
        target_db_path.unlink()

    # Create new database (will use DuckDB 1.3.1 format)
    target_conn = duckdb.connect(str(target_db_path))

    try:
        target_conn.execute(f"ATTACH '{source_db_path}' AS src (READ_ONLY)")

        tables_result = target_conn.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'main'
            AND table_catalog = 'src'
            AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
        """).fetchall()

        exported_tables = []
        exported_views = []

        for table_name, table_type in tables_result:
            if table_type == 'VIEW' and tables_only:
                continue

            target_conn.execute(f"CREATE TABLE main.{table_name} AS SELECT * FROM src.{table_name}")

            if table_type == 'VIEW':
                exported_views.append(table_name)
            else:
                exported_tables.append(table_name)

        target_conn.execute("DETACH src")
        target_conn.close()

        return {
            "source_db": str(source_db_path),
            "target_db": str(target_db_path),
            "exported_tables": exported_tables,
            "exported_views": exported_views,
            "total_exported": len(exported_tables) + len(exported_views),
            "duckdb_version": duckdb.__version__
        }

    except Exception as e:
        target_conn.close()
        raise


def export_reports_command(db_path: str, output_path: Optional[str] = None, json_output: bool = False) -> dict:
    """Command-line interface for exporting reports.

    Args:
        db_path: Path to source database
        output_path: Path to output database (default: reports.duckdb)
        json_output: If True, return result as dict for JSON serialization

    Returns:
        Export statistics dictionary
    """
    output_path = output_path or "reports.duckdb"

    result = export_to_reports_db(db_path, output_path)

    if not json_output:
        print(f"Exported {result['total_exported']} objects to {result['target_db']}")
        print(f"  Tables: {len(result['exported_tables'])}")
        print(f"  Views: {len(result['exported_views'])}")

    return result

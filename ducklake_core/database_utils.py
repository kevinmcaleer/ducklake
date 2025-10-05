"""Database utility functions for DuckDB operations."""
from __future__ import annotations
import duckdb
from typing import Optional, List
from .exceptions import DatabaseOperationError


def get_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    """Get column names for a table or view, with proper error handling."""
    try:
        # Try information_schema first (standard approach)
        result = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name]
        ).fetchall()
        if result:
            return [row[0] for row in result]
    except Exception:
        pass

    try:
        # Fallback to DESCRIBE
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        return [row[0] for row in result]
    except Exception:
        pass

    try:
        # Fallback to PRAGMA (SQLite compatibility)
        result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return [row[1] for row in result]  # Column name is at index 1
    except Exception:
        pass

    return []  # Return empty list if all methods fail


def safe_scalar(conn: duckdb.DuckDBPyConnection, sql: str, params: Optional[List] = None):
    """Execute SQL and return first scalar value, or None if query fails."""
    try:
        row = conn.execute(sql, params or []).fetchone()
        return row[0] if row and len(row) else None
    except Exception:
        return None


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table or view exists."""
    result = safe_scalar(
        conn,
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [table_name]
    )
    return result == 1


def find_timestamp_column(conn: duckdb.DuckDBPyConnection, table_name: str) -> Optional[str]:
    """Find the first available timestamp column in a table."""
    from .config import TimestampField

    columns = get_table_columns(conn, table_name)
    for candidate in TimestampField.all_values():
        if candidate in columns:
            return candidate
    return None


def create_coalesce_expression(columns: List[str], candidates: List[str]) -> Optional[str]:
    """Create a COALESCE expression for the first available columns from candidates."""
    available = [col for col in candidates if col in columns]
    if available:
        return f"COALESCE({', '.join(available)})"
    return None
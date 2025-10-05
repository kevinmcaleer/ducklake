"""SQL file loading utilities for maintaining SQL in separate files."""
from __future__ import annotations
import pathlib
from typing import Dict, Optional
from .exceptions import DatabaseOperationError


def load_sql_file(sql_path: pathlib.Path, **params) -> str:
    """Load SQL file and substitute parameters."""
    try:
        sql_content = sql_path.read_text().strip()

        # Remove comments for cleaner SQL
        lines = []
        for line in sql_content.split('\n'):
            if not line.strip().startswith('--'):
                lines.append(line)
        clean_sql = '\n'.join(lines).strip()

        # Remove trailing semicolon if present (DuckDB COPY doesn't like it)
        if clean_sql.endswith(';'):
            clean_sql = clean_sql[:-1].strip()

        # Substitute parameters
        if params:
            clean_sql = clean_sql.format(**params)

        return clean_sql
    except Exception as e:
        raise DatabaseOperationError(f"Failed to load SQL file {sql_path}: {e}")


def load_schema_sql(sql_dir: pathlib.Path) -> Dict[str, str]:
    """Load all schema SQL files from directory."""
    schema_files = {}
    if not sql_dir.exists():
        return schema_files

    for sql_file in sql_dir.glob('*.sql'):
        schema_files[sql_file.stem] = load_sql_file(sql_file)

    return schema_files


def get_sql_path(relative_path: str) -> pathlib.Path:
    """Get absolute path to SQL file relative to project root."""
    root = pathlib.Path(__file__).resolve().parent.parent
    return root / 'sql' / relative_path


def load_aggregate_sql(aggregate_name: str, **params) -> str:
    """Load specific aggregate SQL file with parameter substitution."""
    sql_path = get_sql_path(f'aggregates/{aggregate_name}.sql')
    return load_sql_file(sql_path, **params)


def load_view_sql(view_name: str, **params) -> str:
    """Load specific view SQL file with parameter substitution."""
    sql_path = get_sql_path(f'views/{view_name}.sql')
    return load_sql_file(sql_path, **params)


def load_schema_sql_file(schema_name: str) -> str:
    """Load specific schema SQL file."""
    sql_path = get_sql_path(f'schema/{schema_name}.sql')
    return load_sql_file(sql_path)
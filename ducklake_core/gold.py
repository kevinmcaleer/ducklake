"""Gold layer rollups for DuckLake."""
from __future__ import annotations

import pathlib

import duckdb

from .utils import _ident, _sql_str


def build_gold_content_rollups(
    conn: duckdb.DuckDBPyConnection,
    lake_root: pathlib.Path,
    source_name: str,
    title_col: str = "name",
    value_col: str = "value",
    agg: str = "sum",
):
    silver_view = f"silver_{source_name}"
    # Check if silver view exists
    try:
        desc = conn.execute(f"DESCRIBE { _ident(silver_view) }").fetchall()
    except duckdb.Error:
        print(f"No silver view for source={source_name}, skipping gold rollups.")
        return

    # Only run gold if title_col exists in the view
    colnames = {row[0] for row in desc}
    if title_col not in colnames:
        print(f"Skipping gold rollup for {source_name}: column '{title_col}' not found in silver view.")
        return

    # Determine column types from silver view description
    type_lookup = {row[0]: (row[1] if len(row) > 1 else "") for row in desc}

    def _is_numeric_type(t: str) -> bool:
        t = (t or "").upper()
        return any(k in t for k in [
            "INT", "DECIMAL", "DOUBLE", "REAL", "HUGEINT", "BIGINT", "SMALLINT", "TINYINT",
            "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT", "FLOAT"
        ])

    if agg.lower() == "count":
        daily_sql = f"""
            SELECT dt, {_ident(title_col)} AS title, COUNT(*) AS total_value
            FROM {_ident(silver_view)}
            GROUP BY 1, 2
            ORDER BY 1 DESC, 3 DESC
        """
        alltime_sql = f"""
            SELECT {_ident(title_col)} AS title, COUNT(*) AS total_value
            FROM {_ident(silver_view)}
            GROUP BY 1
            ORDER BY 2 DESC
        """
    else:
        if value_col not in colnames:
            print(f"Skipping gold rollup for {source_name}: column '{value_col}' not found in silver view.")
            return
        if not _is_numeric_type(type_lookup.get(value_col, "")):
            print(f"INFO: value_col '{value_col}' is non-numeric; using COUNT aggregation for {source_name}.")
            daily_sql = f"""
                SELECT dt, {_ident(title_col)} AS title, COUNT(*) AS total_value
                FROM {_ident(silver_view)}
                GROUP BY 1, 2
                ORDER BY 1 DESC, 3 DESC
            """
            alltime_sql = f"""
                SELECT {_ident(title_col)} AS title, COUNT(*) AS total_value
                FROM {_ident(silver_view)}
                GROUP BY 1
                ORDER BY 2 DESC
            """
        else:
            daily_sql = f"""
                SELECT dt, {_ident(title_col)} AS title, SUM(CAST({_ident(value_col)} AS DOUBLE)) AS total_value
                FROM {_ident(silver_view)}
                GROUP BY 1, 2
                ORDER BY 1 DESC, 3 DESC
            """
            alltime_sql = f"""
                SELECT {_ident(title_col)} AS title, SUM(CAST({_ident(value_col)} AS DOUBLE)) AS total_value
                FROM {_ident(silver_view)}
                GROUP BY 1
                ORDER BY 2 DESC
            """

    gold_dir = lake_root / "gold" / f"source={source_name}"
    gold_dir.mkdir(parents=True, exist_ok=True)
    # Recursively delete output files if present
    for fname in ['daily.parquet', 'all_time.parquet']:
        fpath = gold_dir / fname
        if fpath.exists():
            fpath.unlink()
    conn.execute(
        f"COPY ({daily_sql}) TO '{_sql_str(str(gold_dir / 'daily.parquet'))}' (FORMAT PARQUET)"
    )
    conn.execute(
        f"COPY ({alltime_sql}) TO '{_sql_str(str(gold_dir / 'all_time.parquet'))}' (FORMAT PARQUET)"
    )
    conn.execute(
        f"CREATE OR REPLACE VIEW gold_{_ident(source_name)}_daily AS SELECT * FROM read_parquet('{_sql_str(str(gold_dir / 'daily.parquet'))}', union_by_name=true)"
    )
    conn.execute(
        f"CREATE OR REPLACE VIEW gold_{_ident(source_name)}_all_time AS SELECT * FROM read_parquet('{_sql_str(str(gold_dir / 'all_time.parquet'))}', union_by_name=true)"
    )
    print(f"Gold rollups built for source={source_name} in {gold_dir}")
"""
Gold layer build utilities for DuckLake.
"""
# Placeholder for gold build logic

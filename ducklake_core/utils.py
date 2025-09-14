"""Shared helpers for DuckLake core modules."""
from __future__ import annotations

from typing import Dict, Optional


def _ident(name: str) -> str:
    import re
    out = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    if out and out[0].isdigit():
        out = "_" + out
    return out or "col"


def _col_ref(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_str(val: str) -> str:
    return (val or "").replace("'", "''")


def type_cast_sql(expect_schema: Dict[str, str], src_alias: Optional[str] = None) -> str:
    casts = []
    for col, typ in expect_schema.items():
        t = (typ or "").lower()
        if t in ("string", "text", "str"):
            duck_t = "VARCHAR"
        elif t in ("int", "integer"):
            duck_t = "BIGINT"
        elif t in ("float", "double", "numeric", "decimal"):
            duck_t = "DOUBLE"
        elif t in ("datetime", "timestamp"):
            duck_t = "TIMESTAMP"
        elif t in ("date",):
            duck_t = "DATE"
        else:
            duck_t = "VARCHAR"
        src_col = f"{src_alias}.{_col_ref(col)}" if src_alias else _col_ref(col)
        casts.append(f"CAST({src_col} AS {duck_t}) AS {_ident(col)}")
    return ", ".join(casts) if casts else "*"


def read_sql_for(conn, path_str: str, fmt: str) -> str:
    """Return a SELECT SQL that reads the file at path_str according to fmt."""
    from duckdb import Error as DuckError
    path_ = _sql_str(path_str)
    fmt_l = (fmt or "").lower()
    if fmt_l == "csv":
        return f"SELECT * FROM read_csv_auto('{path_}', ignore_errors=true)"
    if fmt_l == "json":
        base = f"SELECT * FROM read_json_auto('{path_}')"
        try:
            desc = conn.execute(f"DESCRIBE {base}").fetchall()
            list_cols = [c for c, t, *_ in desc if isinstance(t, str) and t.upper().startswith("LIST(")]
            if list_cols:
                lc = list_cols[0]
                return f"SELECT u.* FROM ({base}) t, UNNEST(t.{_col_ref(lc)}) AS u"
        except DuckError:
            pass
        return base
    if fmt_l == "log":
        base_json = f"SELECT * FROM read_json_auto('{path_}')"
        try:
            conn.execute(f"SELECT 1 FROM ({base_json}) LIMIT 1").fetchall()
            return base_json
        except DuckError:
            # Read one row per line using read_csv with explicit single column
            return (
                "SELECT line AS text FROM read_csv("
                f"'{path_}', delim='\n', header=false, columns={{'line':'VARCHAR'}})"
            )
    if fmt_l in ("parquet", "pq"):
        return f"SELECT * FROM read_parquet('{path_}')"
    return f"SELECT * FROM read_csv_auto('{path_}', ignore_errors=true)"

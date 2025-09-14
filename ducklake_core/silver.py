"""Silver layer builder for DuckLake."""
from __future__ import annotations

import pathlib
import shutil
import time
from typing import Dict, Optional

import duckdb

from .manifest import get_manifest_rows
from .utils import _ident, _sql_str, type_cast_sql, read_sql_for, _col_ref


def build_silver_from_manifest(
    conn: duckdb.DuckDBPyConnection,
    lake_root: pathlib.Path,
    source_name: str,
    cfg: Dict,
):
    conn.commit()
    expect_schema: Dict[str, str] = (cfg.get("expect_schema") or {}) if cfg else {}
    parse = (cfg or {}).get("parse") or {}
    tz = (cfg.get("normalize") or {}).get("tz") if cfg else None
    ip_force_octet = (cfg.get("normalize") or {}).get("ip_force_last_octet") if cfg else None
    _ = tz  # tz reserved for future use

    rows = get_manifest_rows(conn, source_name)
    if not rows:
        print(f"No bronze files for source={source_name}")
        return

    selects: list[str] = []
    silver_dir = lake_root / "silver" / f"source={source_name}"
    silver_dir.mkdir(parents=True, exist_ok=True)

    for path_str, fmt, dt in rows:
        p = pathlib.Path(path_str)
        if not p.exists():
            print(f"WARN: Skipping missing file referenced in manifest: {path_str}")
            continue
        read = read_sql_for(conn, path_str, fmt)
        appended = False
        if (fmt or "").lower() == "log":
            # Probe columns
            try:
                cols_probe = [r[0] for r in conn.execute(f"DESCRIBE ({read})").fetchall()]
            except duckdb.Error:
                cols_probe = []
            lower_cols = {c.lower() for c in cols_probe}
            regex = parse.get("regex")
            fields = parse.get("fields") or {}
            ts_fmt = parse.get("ts_format")
            method = (parse.get("method") or "").lower() if isinstance(parse, dict) else ""
            simple = bool(parse.get("simple")) if isinstance(parse, dict) else False
            if regex and isinstance(fields, dict) and fields:
                log_line_col = None
                for c in cols_probe:
                    if c.lower() not in {"dt", "format", "run", "source"}:
                        log_line_col = c
                        break
                if not log_line_col and cols_probe:
                    log_line_col = cols_probe[0]
                read = f"SELECT * FROM (SELECT {log_line_col} AS line FROM ({read})) AS t"
                src_col = "line"
                regex_sql = _sql_str(regex)
                exprs: list[str] = []
                main_field = None
                for name, grp in fields.items():
                    try:
                        gi = int(grp)
                    except (TypeError, ValueError):
                        continue
                    base = f"regexp_extract(t.{src_col}, '{regex_sql}', {gi})"
                    if ts_fmt and name in ("timestamp", "ts"):
                        fmt_sql = _sql_str(ts_fmt)
                        expr = f"strptime(NULLIF(trim({base}), ''), '{fmt_sql}') AS {_ident(name)}"
                        main_field = _ident(name)
                    else:
                        if name == "ip" and ip_force_octet:
                            ip_oct_sql = _sql_str(str(ip_force_octet))
                            expr = (
                                "CASE WHEN NULLIF(trim(" + base + "), '') IS NOT NULL "
                                "THEN concat(split_part(" + base + ", '.', 1), '.', split_part(" + base + ", '.', 2), '.', split_part(" + base + ", '.', 3), '.', '" + ip_oct_sql + "') "
                                "ELSE " + base + " END AS ip"
                            )
                        else:
                            expr = f"{base} AS {_ident(name)}"
                    exprs.append(expr)
                if exprs:
                    if main_field:
                        read = f"SELECT * FROM (SELECT {', '.join(exprs)} FROM ({read}) t) WHERE {main_field} IS NOT NULL"
                    else:
                        read = f"SELECT {', '.join(exprs)} FROM ({read}) t"
                selects.append(f"SELECT *, '{dt}' AS dt FROM ({read}) r")
                appended = True
            elif simple or method == "simple":
                # Non-regex simple parser: assumes format
                # "INFO:root:{ts} - IP: {ip} - Query: {query}"
                log_line_col = None
                for c in cols_probe:
                    if c.lower() not in {"dt", "format", "run", "source"}:
                        log_line_col = c
                        break
                if not log_line_col and cols_probe:
                    log_line_col = cols_probe[0]
                # Prepare a projection using split_part/strptime to avoid regex
                # ts_text = split_part(split_part(line, 'INFO:root:', 2), ' - IP: ', 1)
                # ip      = split_part(split_part(line, ' - IP: ', 2), ' - Query: ', 1)
                # query   = split_part(line, ' - Query: ', 2)
                ts_fmt_sql = _sql_str(ts_fmt or "%Y-%m-%dT%H:%M:%S.%f")
                ip_raw = f"split_part(split_part({log_line_col}, ' - IP: ', 2), ' - Query: ', 1)"
                if ip_force_octet:
                    ip_oct_sql = _sql_str(str(ip_force_octet))
                    ip_expr = (
                        "CASE WHEN NULLIF(trim(" + ip_raw + "), '') IS NOT NULL "
                        "THEN concat(split_part(" + ip_raw + ", '.', 1), '.', split_part(" + ip_raw + ", '.', 2), '.', split_part(" + ip_raw + ", '.', 3), '.', '" + ip_oct_sql + "') "
                        "ELSE " + ip_raw + " END AS ip"
                    )
                else:
                    ip_expr = ip_raw + " AS ip"
                read = (
                    "SELECT "
                    f"strptime(NULLIF(trim(split_part(split_part({log_line_col}, 'INFO:root:', 2), ' - IP: ', 1)), ''), '{ts_fmt_sql}') AS timestamp, "
                    f"{ip_expr}, "
                    f"split_part({log_line_col}, ' - Query: ', 2) AS query "
                    f"FROM ({read})"
                )
                selects.append(f"SELECT *, '{dt}' AS dt FROM ({read}) r")
                appended = True
        # project expected schema for non-log or fallback
        # Discover available columns
        try:
            cols = [r[0] for r in conn.execute(f"DESCRIBE ({read})").fetchall()]
        except duckdb.Error:
            cols = []
        colset = set(cols)
        select_parts: list[str] = []
        if expect_schema:
            for k, v in expect_schema.items():
                if k in colset:
                    duck_t = type_cast_sql({k: v}, src_alias="r")
                    select_parts.append(duck_t)
                else:
                    # missing column -> NULL (leave as is for now)
                    pass
        if not appended:
            projection = ", ".join(select_parts) if select_parts else "*"
            selects.append(f"SELECT {projection}, '{dt}' AS dt FROM ({read}) r")

    if not selects:
        print(f"No usable bronze files for source={source_name}")
        return

    union_sql = "\nUNION ALL\n".join(selects)
    # Deduplicate before writing parquet for sources with primary_key
    primary_key = (cfg or {}).get("primary_key") or []
    # Only use primary_key columns that exist in the data
    try:
        probe_cols = [r[0] for r in conn.execute(f"DESCRIBE ({union_sql})").fetchall()]
    except duckdb.Error:
        probe_cols = []
    pk_actual = [c for c in primary_key if c in probe_cols]
    if pk_actual:
        pk_cols = ", ".join(_ident(c) for c in pk_actual)
        # Choose an order column: prefer 'timestamp' then 'ts' then dt
        order_col = None
        for c in ("timestamp", "ts", "dt"):
            if c in probe_cols:
                order_col = c
                break
        if order_col:
            union_sql = (
                "SELECT * FROM ("
                f"SELECT *, row_number() OVER (PARTITION BY {pk_cols} ORDER BY {_ident(order_col)} DESC) AS _rn "
                f"FROM ({union_sql}) u) WHERE _rn = 1"
            )
    # Ensure dt reflects event date (prefer timestamp -> ts -> existing dt) without referencing missing columns
    try:
        final_probe_cols = [r[0] for r in conn.execute(f"DESCRIBE ({union_sql})").fetchall()]
    except duckdb.Error:
        final_probe_cols = []
    parts: list[str] = []
    if "timestamp" in final_probe_cols:
        parts.append("CAST(timestamp AS DATE)")
    if "ts" in final_probe_cols:
        parts.append("CAST(ts AS DATE)")
    if "dt" in final_probe_cols:
        parts.append("TRY_CAST(dt AS DATE)")
    dt_expr = "COALESCE(" + ", ".join(parts) + ")" if parts else "CAST(NULL AS DATE)"
    union_sql = f"SELECT * REPLACE ({dt_expr} AS dt) FROM ({union_sql})"

    run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = silver_dir / f"run={run}"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_glob = str(out_dir / "part.parquet")
    copy_sql = f"COPY ({union_sql}) TO '{_sql_str(out_glob)}' (FORMAT PARQUET, PARTITION_BY (dt))"
    conn.execute(copy_sql)

    # Build a view across all silver runs; if a primary_key is set, also de-dup at view time
    view_src = f"read_parquet('{_sql_str(str(silver_dir))}/**/*.parquet', union_by_name=true)"
    pk = (cfg or {}).get("primary_key") or []
    # Only use primary_key columns that exist in the view data
    try:
        view_cols = [r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM {view_src}").fetchall()]
    except duckdb.Error:
        view_cols = []
    pk_actual_view = [c for c in pk if c in view_cols]
    if pk_actual_view:
        pk_cols = ", ".join(_ident(c) for c in pk_actual_view)
        order_col = next((c for c in ("timestamp", "ts", "dt") if c in view_cols), "timestamp")
        # Compute dt at view time (prefer timestamp -> ts -> existing dt)
        parts = []
        if "timestamp" in view_cols:
            parts.append(f"CAST(v.timestamp AS DATE)")
        if "ts" in view_cols:
            parts.append(f"CAST(v.ts AS DATE)")
        if "dt" in view_cols:
            parts.append(f"TRY_CAST(v.dt AS DATE)")
        dt_expr = "COALESCE(" + ", ".join(parts) + ")" if parts else "CAST(NULL AS DATE)"
        base_cols = [c for c in view_cols if c not in ("dt", "_rn")]
        proj = ", ".join(f"v.{c}" for c in base_cols) if base_cols else "v.*"
        view_sql = (
            f"CREATE OR REPLACE VIEW silver_{_ident(source_name)} AS "
            f"SELECT {proj}, {dt_expr} AS dt FROM ("
            f"  SELECT u.*, row_number() OVER (PARTITION BY {pk_cols} ORDER BY {order_col} DESC) AS _rn "
            f"  FROM {view_src} u"
            f") v WHERE v._rn = 1"
        )
    else:
        # Simple projection with computed dt, no dedup at view time
        parts = []
        if "timestamp" in view_cols:
            parts.append(f"CAST(u.{_col_ref('timestamp')} AS DATE)")
        if "ts" in view_cols:
            parts.append(f"CAST(u.{_col_ref('ts')} AS DATE)")
        if "dt" in view_cols:
            parts.append(f"TRY_CAST(u.{_col_ref('dt')} AS DATE)")
        dt_expr = "COALESCE(" + ", ".join(parts) + ")" if parts else "CAST(NULL AS DATE)"
        base_cols = [c for c in view_cols if c != "dt"]
        proj = ", ".join(f"u.{_col_ref(c)}" for c in base_cols) if base_cols else "u.*"
        view_sql = (
            f"CREATE OR REPLACE VIEW silver_{_ident(source_name)} AS "
            f"SELECT {proj}, {dt_expr} AS dt FROM {view_src} u"
        )
    conn.execute(view_sql)
    print(f"Silver built for source={source_name} at {silver_dir}")
"""
Silver layer build utilities for DuckLake.
"""
# Placeholder for silver build logic

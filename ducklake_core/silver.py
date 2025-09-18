"""Silver layer builder for DuckLake."""
from __future__ import annotations

import pathlib
import shutil
import time
from typing import Dict, Optional

import duckdb

from .manifest import get_manifest_rows
from .utils import _ident, _sql_str, type_cast_sql, read_sql_for, _col_ref


def build_silver_from_manifest(conn: duckdb.DuckDBPyConnection, lake_root: pathlib.Path, source_name: str, cfg: Dict):
    print(f"[DEBUG] build_silver_from_manifest called with source_name={repr(source_name)}")
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


    from .user_agent_utils import parse_user_agent
    import pandas as pd

    user_agent_cols = [
        "agent_type", "os", "os_version", "browser", "browser_version", "device",
        "is_mobile", "is_tablet", "is_pc", "is_bot"
    ]
    if source_name == "page_count":
        # Clean up all old silver output for this source to avoid schema mismatches
        if silver_dir.exists():
            import shutil
            shutil.rmtree(silver_dir)
        silver_dir.mkdir(parents=True, exist_ok=True)
        # Use a single consistent run directory for all output
        run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        for path_str, fmt, dt in rows:
            p = pathlib.Path(path_str)
            if p.name.startswith('.'):
                print(f"SKIP: Hidden/system file in manifest: {path_str}")
                continue
            if not p.exists():
                print(f"WARN: Skipping missing file referenced in manifest: {path_str}")
                continue
            read = read_sql_for(conn, path_str, fmt)
            # Load to pandas for enrichment
            df = conn.execute(f"SELECT *, '{dt}' AS dt FROM ({read}) r").df()
            if "user_agent" not in df.columns:
                print(f"[WARN] user_agent column missing in file {path_str}; adding empty strings")
                df["user_agent"] = ""
            enrich = df["user_agent"].fillna("").apply(parse_user_agent)
            for col in user_agent_cols:
                df[col] = enrich.apply(lambda d: d.get(col))
            out_dir = silver_dir / f"run={run}" / "part.parquet" / f"dt={dt}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / "data_0.parquet"
            print(f"[DEBUG] Columns in enriched DataFrame for dt={dt}: {list(df.columns)}")
            df.to_parquet(out_file, index=False)
        # Now build the view from this run
        out_dir = silver_dir / f"run={run}"
        view_src = f"read_parquet('{_sql_str(str(out_dir))}/**/*.parquet', union_by_name=true)"
        view_cols = None
        try:
            view_cols = [r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM {view_src}").fetchall()]
        except duckdb.Error:
            view_cols = []
        base_cols = [c for c in view_cols if c != "dt"]
        proj = ", ".join(f"u.{_col_ref(c)}" for c in base_cols) if base_cols else "u.*"
        view_sql = (
            f"CREATE OR REPLACE VIEW silver_{_ident(source_name)} AS "
            f"SELECT {proj}, TRY_CAST(u.dt AS DATE) AS dt FROM {view_src} u"
        )
        conn.execute(view_sql)
    print(f"Silver built for source={source_name} at {out_dir}")
    return

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

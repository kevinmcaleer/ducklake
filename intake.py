import argparse
import hashlib
import json
import pathlib
import sys
import time
from datetime import date, datetime
from typing import Dict, Optional

import duckdb
import re

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

ROOT = pathlib.Path(__file__).resolve().parent
LAKE = ROOT
DUCKDB_DIR = ROOT / "duckdb"
DUCKDB_DIR.mkdir(parents=True, exist_ok=True)
DB = str(DUCKDB_DIR / "lake.duckdb")
conn = duckdb.connect(DB)

# Manifest for bronze files
conn.execute(
    """
CREATE TABLE IF NOT EXISTS manifest (
  source TEXT,
  run_ts TIMESTAMP,
  dt DATE,
  path TEXT,
  bytes BIGINT,
  content_sha256 TEXT,
  rows BIGINT,
  format TEXT,
  meta JSON
)
"""
)


def sha256_file(p: pathlib.Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_sources_config(cfg_path: pathlib.Path) -> Dict:
    if not cfg_path.exists():
        return {}
    if yaml is None:
        raise RuntimeError("PyYAML is required to read configs/sources.yml. Install with: uv pip install pyyaml")
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def ensure_dirs():
    for d in (LAKE / "bronze", LAKE / "silver", LAKE / "gold"):
        d.mkdir(parents=True, exist_ok=True)


def _ident(name: str) -> str:
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


def ingest_bronze(source_name: str, raw_path: str, fmt: str, dt_override: Optional[str] = None, meta: Optional[Dict] = None):
    p = pathlib.Path(raw_path)
    if not p.exists():
        raise FileNotFoundError(raw_path)
    dt_ = dt_override or date.today().isoformat()
    try:
        dt_ = datetime.fromisoformat(dt_).date().isoformat()
    except (TypeError, ValueError):
        pass
    run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    dest_dir = LAKE / "bronze" / f"source={source_name}" / f"format={fmt.lower()}" / f"dt={dt_}" / f"run={run}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / p.name

    fmt_l = fmt.lower()
    original_name = p.name
    if fmt_l in ("yaml", "yml"):
        if yaml is None:
            raise RuntimeError("PyYAML is required to ingest YAML. Install with: uv pip install pyyaml")
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            if "items" in data and isinstance(data["items"], list):
                records = data["items"]
            else:
                records = [data]
        elif isinstance(data, list):
            records = data
        else:
            records = [{"value": data}]
        json_path = dest.with_suffix(".json")
        with json_path.open("w", encoding="utf-8") as jf:
            for rec in records:
                jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
        orig_copy = dest.with_suffix(".yaml")
        orig_copy.write_bytes(p.read_bytes())
        dest = json_path
        fmt_l = "json"
        fmt = "json"
        meta = {**(meta or {}), "original_format": "yaml", "original_name": original_name}
    else:
        dest.write_bytes(p.read_bytes())

    digest = sha256_file(dest)
    exists = conn.execute("SELECT 1 FROM manifest WHERE content_sha256 = ?", [digest]).fetchone()
    if exists:
        return str(dest), "SKIPPED_DUPLICATE"

    rows = 0
    if fmt_l == "csv":
        res = conn.execute(f"SELECT count(*) FROM read_csv_auto('{_sql_str(str(dest))}')").fetchone()
        rows = res[0] if res else 0
    elif fmt_l == "json":
        res = conn.execute(f"SELECT count(*) FROM read_json_auto('{_sql_str(str(dest))}')").fetchone()
        rows = res[0] if res else 0
    elif fmt_l in ("parquet", "pq"):
        res = conn.execute(f"SELECT count(*) FROM read_parquet('{_sql_str(str(dest))}')").fetchone()
        rows = res[0] if res else 0

    conn.execute(
        """
      INSERT INTO manifest VALUES (?, now(), ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            source_name,
            dt_,
            str(dest),
            dest.stat().st_size,
            digest,
            rows,
            fmt,
            json.dumps(meta or {}),
        ],
    )
    return str(dest), "INGESTED"


def _read_sql_for(path_str: str, fmt: str) -> str:
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
        except duckdb.Error:
            pass
        return base
    if fmt_l == "log":
        # Try JSON lines first, else fallback to plain text lines
        base_json = f"SELECT * FROM read_json_auto('{path_}')"
        try:
            conn.execute(f"SELECT 1 FROM ({base_json}) LIMIT 1").fetchall()
            return base_json
        except duckdb.Error:
            # DuckDB read_text returns 'content' column in many versions; alias to a normalized 'text'
            return f"SELECT content AS text FROM read_text('{path_}')"
    if fmt_l in ("parquet", "pq"):
        return f"SELECT * FROM read_parquet('{path_}')"
    return f"SELECT * FROM read_csv_auto('{path_}', ignore_errors=true)"


def build_silver_from_manifest(source_name: str, cfg: Dict):
    expect_schema: Dict[str, str] = (cfg.get("expect_schema") or {}) if cfg else {}
    tz = (cfg.get("normalize") or {}).get("tz") if cfg else None
    primary_key = cfg.get("primary_key") if cfg else None

    rows = conn.execute(
        """
        SELECT path, format, dt
        FROM manifest
        WHERE source = ?
        ORDER BY run_ts
        """,
        [source_name],
    ).fetchall()
    if not rows:
        print(f"No bronze files for source={source_name}")
        return

    selects: list[str] = []
    for path_str, fmt, dt in rows:
        read = _read_sql_for(path_str, fmt)
        # Optional regex-based parsing for plain text logs
        if (fmt or "").lower() == "log":
            try:
                cols_probe = [r[0] for r in conn.execute(f"DESCRIBE ({read})").fetchall()]
            except duckdb.Error:
                cols_probe = []
            lower_cols = {c.lower() for c in cols_probe}
            if "text" in lower_cols or "content" in lower_cols:
                parse = (cfg or {}).get("parse") or {}
                regex = parse.get("regex")
                fields = parse.get("fields") or {}
                ts_fmt = parse.get("ts_format")
                if regex and isinstance(fields, dict) and fields:
                    regex_sql = _sql_str(regex)
                    src_col = "text" if "text" in lower_cols else "content"
                    exprs: list[str] = []
                    for name, grp in fields.items():
                        try:
                            gi = int(grp)
                        except (TypeError, ValueError):
                            continue
                        base = f"regexp_extract(t.{src_col}, '{regex_sql}', {gi})"
                        if ts_fmt and name in ("timestamp", "ts"):
                            fmt_sql = _sql_str(ts_fmt)
                            expr = f"strptime(NULLIF(trim({base}), ''), '{fmt_sql}') AS {_ident(name)}"
                        else:
                            expr = f"{base} AS {_ident(name)}"
                        exprs.append(expr)
                    if exprs:
                        read = f"SELECT {', '.join(exprs)} FROM ({read}) t"
        # Discover available columns in this read
        try:
            cols = [r[0] for r in conn.execute(f"DESCRIBE ({read})").fetchall()]
        except duckdb.Error:
            cols = []
        colset = set(cols)

        # choose ts column name that actually exists
        ts_candidates = ["ts", "timestamp", "time", "date"]
        ts_col: Optional[str] = next((c for c in ts_candidates if c in colset), None)

        # Build select list: for each expected column, cast if exists, else NULL as placeholder
        select_parts: list[str] = []
        if expect_schema:
            for k, v in expect_schema.items():
                if k in colset:
                    duck_t = type_cast_sql({k: v}, src_alias="r")
                    # type_cast_sql returns 'CAST(r."k" AS TYPE) AS k'
                    select_parts.append(duck_t)
                else:
                    # missing column -> NULL
                    t = (v or "").lower()
                    if t in ("datetime", "timestamp"):
                        null_t = "TIMESTAMP"
                    elif t in ("date",):
                        null_t = "DATE"
                    elif t in ("int", "integer"):
                        null_t = "BIGINT"
                    elif t in ("float", "double", "numeric", "decimal"):
                        null_t = "DOUBLE"
                    else:
                        null_t = "VARCHAR"
                    select_parts.append(f"CAST(NULL AS {null_t}) AS {_ident(k)}")
        else:
            select_parts.append("r.*")

        # ts and dt expressions
        if ts_col and ts_col in colset:
            if tz:
                tz_lit = _sql_str(tz)
                ts_expr = f"(CAST(r.{_col_ref(ts_col)} AS TIMESTAMP) AT TIME ZONE '{tz_lit}') AT TIME ZONE 'UTC' AS ts"
                dt_expr = f"CAST((CAST(r.{_col_ref(ts_col)} AS TIMESTAMP) AT TIME ZONE '{tz_lit}') AS DATE) AS dt"
            else:
                ts_expr = f"CAST(r.{_col_ref(ts_col)} AS TIMESTAMP) AS ts"
                dt_expr = f"CAST(r.{_col_ref(ts_col)} AS DATE) AS dt"
        else:
            ts_expr = "CAST(NULL AS TIMESTAMP) AS ts"
            dt_expr = f"CAST('{_sql_str(str(dt))}' AS DATE) AS dt"

        select_list = ", ".join(select_parts) if select_parts else "r.*"
        selects.append(f"SELECT {select_list}, {ts_expr}, {dt_expr} FROM ({read}) r")

    union_sql = "\nUNION ALL\n".join(selects)

    if primary_key:
        pk_cols = ", ".join([_ident(c) for c in primary_key])
        union_sql = f"SELECT * FROM (\n{union_sql}\n) t QUALIFY row_number() OVER (PARTITION BY {pk_cols} ORDER BY ts DESC NULLS LAST) = 1"


    import shutil
    silver_dir = LAKE / "silver" / f"source={source_name}"
    silver_dir.mkdir(parents=True, exist_ok=True)
    run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = silver_dir / f"run={run}"
    # Recursively delete output dir if it exists
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_glob = str(out_dir / "part.parquet")
    copy_sql = f"COPY ({union_sql}) TO '{_sql_str(out_glob)}' (FORMAT PARQUET, PARTITION_BY (dt))"
    conn.execute(copy_sql)

    view_sql = f"CREATE OR REPLACE VIEW silver_{_ident(source_name)} AS SELECT * FROM read_parquet('{_sql_str(str(silver_dir))}/**/*.parquet')"
    conn.execute(view_sql)
    print(f"Silver built for source={source_name} at {silver_dir}")


def build_gold_content_rollups(source_name: str, title_col: str = "name", value_col: str = "value", agg: str = "sum"):
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

    gold_dir = LAKE / "gold" / f"source={source_name}"
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
        f"CREATE OR REPLACE VIEW gold_{_ident(source_name)}_daily AS SELECT * FROM read_parquet('{_sql_str(str(gold_dir / 'daily.parquet'))}')"
    )
    conn.execute(
        f"CREATE OR REPLACE VIEW gold_{_ident(source_name)}_all_time AS SELECT * FROM read_parquet('{_sql_str(str(gold_dir / 'all_time.parquet'))}')"
    )
    print(f"Gold rollups built for source={source_name} in {gold_dir}")


def cli():
    parser = argparse.ArgumentParser(description="DuckDB lakehouse intake: bronze -> silver -> gold")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ingest = sub.add_parser("ingest-file", help="Ingest a file into bronze")
    p_ingest.add_argument("source", help="Logical source name (matches configs/sources.yml)")
    p_ingest.add_argument("path", help="Path to file (csv/json/parquet)")
    p_ingest.add_argument("--format", "-f", default=None, help="csv|json|parquet (auto from extension if omitted)")
    p_ingest.add_argument("--dt", default=None, help="Partition date YYYY-MM-DD")
    p_ingest.add_argument("--meta", default=None, help="JSON string of extra metadata")

    p_ingest_sqlite = sub.add_parser("ingest-sqlite", help="Ingest a SQLite table into bronze (exports as parquet)")
    p_ingest_sqlite.add_argument("source", help="Logical source name")
    p_ingest_sqlite.add_argument("db", help="Path to SQLite .db file")
    p_ingest_sqlite.add_argument("table", help="SQLite table name to extract")
    p_ingest_sqlite.add_argument("--dt", default=None)

    p_ingest_url = sub.add_parser("ingest-url", help="Fetch a URL and ingest response into bronze")
    p_ingest_url.add_argument("source", help="Logical source name")
    p_ingest_url.add_argument("url", help="HTTP/HTTPS URL to fetch")
    p_ingest_url.add_argument("--format", "-f", default=None, help="csv|json|parquet|yaml; inferred from Content-Type or URL if omitted")
    p_ingest_url.add_argument("--dt", default=None)
    p_ingest_url.add_argument("--insecure", action="store_true", help="Disable SSL certificate verification (use with caution)")

    p_silver = sub.add_parser("silver", help="Build silver dataset for a source")
    p_silver.add_argument("source", help="Logical source name")

    p_gold = sub.add_parser("gold", help="Build gold rollups for a source")
    p_gold.add_argument("source", help="Logical source name")
    p_gold.add_argument("--title-col", default="name")
    p_gold.add_argument("--value-col", default="value")
    p_gold.add_argument("--agg", choices=["sum", "count"], default="sum", help="Aggregation: sum (default) or count")

    sub.add_parser("refresh", help="Build silver and gold for all sources in configs/sources.yml")

    args = parser.parse_args()
    ensure_dirs()
    cfg = load_sources_config(ROOT / "configs" / "sources.yml")

    if args.cmd == "ingest-file":
        fmt = args.format or pathlib.Path(args.path).suffix.lstrip(".")
        meta = json.loads(args.meta) if args.meta else None
        dest, status = ingest_bronze(args.source, args.path, fmt, dt_override=args.dt, meta=meta)
        print(json.dumps({"dest": dest, "status": status}, indent=2))
    elif args.cmd == "ingest-sqlite":
        sqlite_db = pathlib.Path(args.db)
        if not sqlite_db.exists():
            raise FileNotFoundError(args.db)
        try:
            conn.execute("INSTALL sqlite; LOAD sqlite;")
        except Exception as e:
            raise RuntimeError("DuckDB sqlite extension is required. Ensure network access or preinstalled extension.") from e
        run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        tmp_parquet = ROOT / f"_tmp_sqlite_export_{run}.parquet"
        export_sql = (
            f"COPY (SELECT * FROM sqlite_scan('{_sql_str(str(sqlite_db))}', '{_sql_str(args.table)}')) "
            f"TO '{_sql_str(str(tmp_parquet))}' (FORMAT PARQUET, ALLOW_OVERWRITE TRUE)"
        )
        conn.execute(export_sql)
        dest, status = ingest_bronze(args.source, str(tmp_parquet), "parquet", dt_override=args.dt, meta={"sqlite_db": str(sqlite_db), "table": args.table})
        try:
            tmp_parquet.unlink(missing_ok=True)
        except FileNotFoundError:
            pass
        print(json.dumps({"dest": dest, "status": status}, indent=2))
    elif args.cmd == "ingest-url":
        import urllib.request
        from urllib.parse import urlparse
        import ssl as _ssl
        cafile = None
        try:
            import certifi  # type: ignore
            cafile = certifi.where()
        except ImportError:
            cafile = None

        u = args.url
        run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        tmp_dir = ROOT / "_tmp_downloads"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        parsed = urlparse(u)
        name = pathlib.Path(parsed.path).name or f"download_{run}"
        tmp_path = tmp_dir / name
        req = urllib.request.Request(u, headers={"User-Agent": "ducklake/1.0"})
        if args.insecure:
            ctx = _ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = _ssl.CERT_NONE
        else:
            ctx = _ssl.create_default_context()
            if cafile:
                ctx.load_verify_locations(cafile=cafile)
        with urllib.request.urlopen(req, context=ctx) as resp:  # nosec B310
            data = resp.read()
            ctype = resp.headers.get("Content-Type", "").lower()
        tmp_path.write_bytes(data)

        # If JSON, check for top-level array or array under a key (e.g., 'visits')
        fmt = args.format
        if not fmt:
            if "/json" in ctype or name.endswith(".json"):
                fmt = "json"
            elif "/csv" in ctype or name.endswith(".csv"):
                fmt = "csv"
            elif "parquet" in ctype or name.endswith(".parquet"):
                fmt = "parquet"
            elif "yaml" in ctype or name.endswith((".yaml", ".yml")):
                fmt = "yaml"
            else:
                fmt = "csv"

        # Special handling for JSON: flatten top-level array or array under a key
        used_flattened = False
        if fmt == "json":
            try:
                text = data.decode("utf-8")
                js = json.loads(text)
                # If it's a dict with a single key and value is a list, flatten
                if isinstance(js, dict):
                    for k in ("visits", "items", "data", "records"):
                        if k in js and isinstance(js[k], list):
                            arr = js[k]
                            flat_path = tmp_path.with_suffix(".jsonl")
                            with flat_path.open("w", encoding="utf-8") as f:
                                for rec in arr:
                                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            orig_path = tmp_path.with_suffix(".orig.json")
                            orig_path.write_bytes(data)
                            tmp_path = flat_path
                            used_flattened = True
                            break
                elif isinstance(js, list):
                    flat_path = tmp_path.with_suffix(".jsonl")
                    with flat_path.open("w", encoding="utf-8") as f:
                        for rec in js:
                            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    orig_path = tmp_path.with_suffix(".orig.json")
                    orig_path.write_bytes(data)
                    tmp_path = flat_path
                    used_flattened = True
            except Exception as e:
                print(f"WARN: Could not flatten JSON: {e}")

        # Always use .jsonl if it was created
        if fmt == "json" and used_flattened:
            bronze_path = tmp_path
        else:
            bronze_path = tmp_path

        dest, status = ingest_bronze(args.source, str(bronze_path), fmt, dt_override=args.dt, meta={"source_url": u, "content_type": ctype})
        print(json.dumps({"dest": dest, "status": status}, indent=2))
    elif args.cmd == "silver":
        build_silver_from_manifest(args.source, (cfg or {}).get(args.source, {}))
    elif args.cmd == "gold":
        build_silver_from_manifest(args.source, (cfg or {}).get(args.source, {}))
        build_gold_content_rollups(args.source, title_col=args.title_col, value_col=args.value_col, agg=args.agg)
    elif args.cmd == "refresh":
        for src_name, src_cfg in (cfg or {}).items():
            build_silver_from_manifest(src_name, src_cfg)
            schema = (src_cfg or {}).get("expect_schema") or {}
            title_col = "name" if "name" in schema else (next(iter(schema.keys()), "id"))
            value_col = "value" if "value" in schema else (
                next((k for k, v in schema.items() if str(v).lower() in ("int", "integer", "float", "double", "numeric", "decimal")), next(iter(schema.keys()), "id"))
            )
            build_gold_content_rollups(src_name, title_col=title_col, value_col=value_col)


if __name__ == "__main__":
    try:
        cli()
    except KeyboardInterrupt:
        sys.exit(130)

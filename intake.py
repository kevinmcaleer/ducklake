import argparse
import hashlib
import json
import pathlib
import sys
import time
import shutil
from datetime import date, datetime
from typing import Dict, Optional

import duckdb
import re
from ducklake_core.bronze import ingest_bronze as bronze_ingest, backfill_manifest_from_bronze as bronze_backfill

# Modular imports
from ducklake_core.manifest import get_manifest_rows, print_manifest
from ducklake_core.silver import build_silver_from_manifest as silver_build
from ducklake_core.gold import build_gold_content_rollups as gold_build
from ducklake_core.report_views import create_report_views

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

import logging

ROOT = pathlib.Path(__file__).resolve().parent
LAKE = ROOT
DUCKDB_DIR = ROOT / "duckdb_utils"
DUCKDB_DIR.mkdir(parents=True, exist_ok=True)
DB = str(DUCKDB_DIR / "lake.duckdb")
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("ducklake")
logger.setLevel(logging.ERROR)
logger.debug(f"Using DuckDB database at: {DB}")
conn = duckdb.connect(DB)

# Print all sources in manifest for debug
try:
    manifest_sources = conn.execute("SELECT source, dt, path, format, rows FROM manifest").fetchall()
    logger.debug(f"manifest table rows at startup: {manifest_sources}")
except Exception as e:
    logger.debug(f"Could not read manifest table: {e}")

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
    """Wrapper calling bronze.ingest with current connection and lake root."""
    return bronze_ingest(conn, LAKE, source_name, raw_path, fmt, dt_override=dt_override, meta=meta)


def backfill_manifest_from_bronze(source_name: str):
    """Wrapper calling bronze.backfill with current connection and lake root."""
    return bronze_backfill(conn, LAKE, source_name)


def ingest_from_config(source_name: str, src_cfg: Dict):
    """Ingest latest data for a source based on its config url/format.

    - http(s): fetch and ingest (flatten JSON arrays heuristically)
    - file path: copy into bronze with configured format
    Skips duplicates by content hash automatically via bronze ingest.
    """
    url = (src_cfg or {}).get("url")
    fmt_cfg = (src_cfg or {}).get("format")
    if not url:
        return
    try:
        # Local file path
        p = pathlib.Path(url)
        if p.exists():
            fmt = (fmt_cfg or p.suffix.lstrip(".") or "csv").lower()
            # Show progress bar for file read
            file_size = p.stat().st_size
            with p.open("rb") as f, tqdm.tqdm(total=file_size, unit="B", unit_scale=True, desc=f"Ingesting {p.name}") as bar:
                chunk_size = 1024 * 1024
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    bar.update(len(chunk))
            ingest_bronze(source_name, str(p), fmt)
            return
    except Exception:
        pass
    # HTTP/HTTPS
    if isinstance(url, str) and (url.startswith("http://") or url.startswith("https://")):
        import urllib.request
        from urllib.parse import urlparse
        import ssl as _ssl
        cafile = None
        try:
            import certifi  # type: ignore
            cafile = certifi.where()
        except Exception:
            cafile = None
        run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        tmp_dir = ROOT / "_tmp_downloads"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        parsed = urlparse(url)
        name = pathlib.Path(parsed.path).name or f"download_{run}"
        tmp_path = tmp_dir / name
        req = urllib.request.Request(url, headers={"User-Agent": "ducklake/1.0"})
        ctx = _ssl.create_default_context()
        if cafile:
            try:
                ctx.load_verify_locations(cafile=cafile)
            except Exception:
                pass
        with urllib.request.urlopen(req, context=ctx) as resp:  # nosec B310
            total = int(resp.headers.get("Content-Length", 0))
            data = bytearray()
            with tqdm.tqdm(total=total, unit="B", unit_scale=True, desc=f"Downloading {name}") as bar:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    data.extend(chunk)
                    bar.update(len(chunk))
            ctype = resp.headers.get("Content-Type", "").lower()
        tmp_path.write_bytes(data)
        fmt = (fmt_cfg or ("json" if "/json" in ctype or name.endswith(".json") else ("csv" if "/csv" in ctype or name.endswith(".csv") else ("parquet" if "parquet" in ctype or name.endswith(".parquet") else ("yaml" if "yaml" in ctype or name.endswith((".yaml", ".yml")) else "csv")))))
        # Flatten common JSON envelope shapes to .jsonl
        used_flattened = False
        if fmt == "json":
            try:
                text = data.decode("utf-8")
                js = json.loads(text)
                if isinstance(js, dict):
                    for k in ("visits", "items", "data", "records"):
                        if k in js and isinstance(js[k], list):
                            arr = js[k]
                            flat_path = tmp_path.with_suffix(".jsonl")
                            with flat_path.open("w", encoding="utf-8") as f:
                                for rec in arr:
                                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            tmp_path = flat_path
                            used_flattened = True
                            break
                elif isinstance(js, list):
                    flat_path = tmp_path.with_suffix(".jsonl")
                    with flat_path.open("w", encoding="utf-8") as f:
                        for rec in js:
                            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    tmp_path = flat_path
                    used_flattened = True
            except Exception:
                pass
        ingest_bronze(source_name, str(tmp_path), fmt, meta={"source_url": url})


def run_reports_sql(conn: duckdb.DuckDBPyConnection):
    """Execute the SQL script that writes CSV reports to ./reports."""
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    sql_path = ROOT / "sql" / "reports.sql"
    if not sql_path.exists():
        logger.warning(f"reports.sql not found at {sql_path}")
        return
    txt = sql_path.read_text(encoding="utf-8")
    # Naive split on semicolons; sufficient for our simple COPY statements
    for stmt in txt.split(";"):
        s = stmt.strip()
        if not s:
            continue
        try:
            conn.execute(s)
        except Exception as e:
            logger.warning(f"Failed to execute report statement: {str(e)}\nSQL: {s[:200]}...")


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
    # Ensure all manifest changes are committed and visible
    conn.commit()
    expect_schema: Dict[str, str] = (cfg.get("expect_schema") or {}) if cfg else {}
    tz = (cfg.get("normalize") or {}).get("tz") if cfg else None
    primary_key = cfg.get("primary_key") if cfg else None

    logger.debug(f"source_name repr: {repr(source_name)}")
    manifest_dump = conn.execute("SELECT source, dt, path, format, rows FROM manifest").fetchall()
    logger.debug(f"manifest table rows before query: {manifest_dump}")
    manifest_sources = conn.execute("SELECT DISTINCT source FROM manifest").fetchall()
    logger.debug(f"manifest sources repr: {[repr(s[0]) for s in manifest_sources]}")
    rows = get_manifest_rows(conn, source_name)
    logger.debug(f"Manifest query returned rows: {rows}")
    if not rows:
        logger.info(f"No bronze files for source={source_name}")
        return

    selects: list[str] = []
    missing_count = 0
    silver_dir = LAKE / "silver" / f"source={source_name}"
    silver_dir.mkdir(parents=True, exist_ok=True)
    import os
    for path_str, fmt, dt in rows:
        import os
        exists = os.path.exists(path_str)
        logger.debug(f"ROW: path={path_str}, format={fmt}, dt={dt}, exists={exists}")
        if not exists:
            logger.debug(f"SKIP: File does not exist: {path_str}")
            continue
        logger.debug(f"PROCESS: File will be processed: {path_str}")
        logger.debug(f"Considering file: {path_str} (format={fmt}, dt={dt})")
        # Skip entries whose files are no longer present on disk
        try:
            p = pathlib.Path(path_str)
            if not p.exists():
                missing_count += 1
                logger.warning(f"Skipping missing file referenced in manifest: {path_str}")
                continue
        except Exception:
            missing_count += 1
            logger.warning(f"Skipping invalid path in manifest: {path_str}")
            continue
        logger.debug(f"File exists and will be processed: {path_str}")
        read = _read_sql_for(path_str, fmt)
        appended = False
        # Always attempt regex-based parsing for log files, even if only one column exists
        if (fmt or "").lower() == "log":
            try:
                cols_probe = [r[0] for r in conn.execute(f"DESCRIBE ({read})").fetchall()]
            except duckdb.Error:
                cols_probe = []
            lower_cols = {c.lower() for c in cols_probe}
            parse = (cfg or {}).get("parse") or {}
            regex = parse.get("regex")
            fields = parse.get("fields") or {}
            ts_fmt = parse.get("ts_format")
            # If the log is read as a single column, always alias as 'line' for regex extraction
            logger.debug(f"regex={regex}, fields={fields}, type={type(fields)}")
            if regex and isinstance(fields, dict) and fields:
                # Always alias the log line column as 'line' for regex extraction
                log_line_col = None
                for c in cols_probe:
                    if c.lower() not in {"dt", "format", "run", "source"}:
                        log_line_col = c
                        break
                if not log_line_col and cols_probe:
                    log_line_col = cols_probe[0]
                read = f"SELECT * FROM (SELECT {log_line_col} AS line FROM ({read})) AS t"
                src_col = "line"
                # Build regex extraction SQL for log files
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
                        expr = f"{base} AS {_ident(name)}"
                    exprs.append(expr)
                if exprs:
                    # Only keep rows where the main parsed field (timestamp/ts) is not NULL
                    if main_field:
                        read = f"SELECT * FROM (SELECT {', '.join(exprs)} FROM ({read}) t) WHERE {main_field} IS NOT NULL"
                    else:
                        read = f"SELECT {', '.join(exprs)} FROM ({read}) t"
                # Add the final read SQL to selects for union
                selects.append(f"SELECT *, '{dt}' AS dt FROM ({read}) r")
                appended = True
            else:
                # Find the first column to use as the source text (always use first if text/content not present)
                src_col = None
                for candidate in ("text", "content"):
                    if candidate in lower_cols:
                        src_col = candidate
                        break
                if not src_col and cols_probe:
                    src_col = cols_probe[0]
                if src_col:
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
                            expr = f"{base} AS {_ident(name)}"
                        exprs.append(expr)
                    if exprs:
                        # Only keep rows where the main parsed field (timestamp/ts) is not NULL
                        if main_field:
                            read = f"SELECT * FROM (SELECT {', '.join(exprs)} FROM ({read}) t) WHERE {main_field} IS NOT NULL"
                        else:
                            read = f"SELECT {', '.join(exprs)} FROM ({read}) t"
                # If not appended yet, append generic projection below
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
                    if (fmt or "").lower() == "log":
                        parse = (cfg or {}).get("parse") or {}
                        regex = parse.get("regex")
                        ts_fmt = parse.get("ts_format")
                        # For log files, always use the proven SQL pattern: read as 'line', then extract
                        if regex:
                            regex_sql = _sql_str(regex)
                            # Use the expected field mapping for search_logs
                            # If ts_fmt is set, parse timestamp
                            if ts_fmt:
                                fmt_sql = _sql_str(ts_fmt)
                                read = f"SELECT strptime(NULLIF(trim(regexp_extract(line, '{regex_sql}', 1)), ''), '{fmt_sql}') AS timestamp, regexp_extract(line, '{regex_sql}', 2) AS ip, regexp_extract(line, '{regex_sql}', 3) AS query FROM (SELECT * FROM read_csv_auto('{_sql_str(path_str)}', delim='\n'))"
                            else:
                                read = f"SELECT regexp_extract(line, '{regex_sql}', 1) AS timestamp, regexp_extract(line, '{regex_sql}', 2) AS ip, regexp_extract(line, '{regex_sql}', 3) AS query FROM (SELECT * FROM read_csv_auto('{_sql_str(path_str)}', delim='\n'))"
                        # If not appended yet, append generic projection below
        # Append a SELECT for this file if not already added in log branch
        if not appended:
            projection = ", ".join(select_parts) if select_parts else "*"
            selects.append(f"SELECT {projection}, '{dt}' AS dt FROM ({read}) r")
    # After the for-loop, check if any selects were built
    if not selects:
        msg = f"No usable bronze files for source={source_name}"
        if missing_count:
            msg += f" (skipped {missing_count} missing)"
        logger.info(msg)
        return
    # Delegate to silver module
    return silver_build(conn, LAKE, source_name, cfg)


def build_gold_content_rollups(source_name: str, title_col: str = "name", value_col: str = "value", agg: str = "sum"):
    return gold_build(conn, LAKE, source_name, title_col=title_col, value_col=value_col, agg=agg)


try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init()
    GREEN = Fore.GREEN
    RED = Fore.RED
    RESET = Style.RESET_ALL
except ImportError:
    GREEN = ""
    RED = ""
    RESET = ""

CHECK = f"{GREEN}✔️{RESET}"
CROSS = f"{RED}❌{RESET}"


def print_task_status(task, status):
    if status == "ok":
        print(f"  {CHECK} {task}")
    elif status == "fail":
        print(f"  {CROSS} {task}")
    else:
        print(f"  [ ] {task}")


def print_main_task(task):
    print(f"\n{task}:")


def print_subtask_status(subtask, status):
    if status == "ok":
        print(f"    {CHECK} {subtask}")
    elif status == "fail":
        print(f"    {CROSS} {subtask}")
    else:
        print(f"    [ ] {subtask}")


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
        logger.info(json.dumps({"dest": dest, "status": status}, indent=2))
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
        logger.info(json.dumps({"dest": dest, "status": status}, indent=2))
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
                logger.warning(f"Could not flatten JSON: {e}")

        # Always use .jsonl if it was created
        if fmt == "json" and used_flattened:
            bronze_path = tmp_path
        else:
            bronze_path = tmp_path

        dest, status = ingest_bronze(args.source, str(bronze_path), fmt, dt_override=args.dt, meta={"source_url": u, "content_type": ctype})
        logger.info(json.dumps({"dest": dest, "status": status}, indent=2))
    elif args.cmd == "silver":
        backfill_manifest_from_bronze(args.source)
        # Print manifest table before building silver
        manifest_dump = conn.execute("SELECT source, dt, path, format, rows FROM manifest").fetchall()
        logger.debug(f"manifest table rows before build_silver_from_manifest: {manifest_dump}")
        build_silver_from_manifest(args.source, ((cfg or {}).get(args.source, {})))
    elif args.cmd == "gold":
        backfill_manifest_from_bronze(args.source)
        build_silver_from_manifest(args.source, ((cfg or {}).get(args.source, {})))
        build_gold_content_rollups(args.source, title_col=args.title_col, value_col=args.value_col, agg=args.agg)
    elif args.cmd == "refresh":
        # Group tasks by layer
        bronze_tasks = []
        silver_tasks = []
        gold_tasks = []
        for src_name, src_cfg in (cfg or {}).items():
            bronze_tasks.append((src_name, [
                ("Ingest", lambda: ingest_from_config(src_name, src_cfg)),
                ("Backfill manifest", lambda: backfill_manifest_from_bronze(src_name)),
            ]))
            silver_tasks.append((src_name, [
                ("Build silver", lambda: build_silver_from_manifest(src_name, src_cfg)),
            ]))
            schema = (src_cfg or {}).get("expect_schema") or {}
            title_col = None
            if "name" in schema:
                title_col = "name"
            elif "url" in schema:
                title_col = "url"
            else:
                string_cols = [k for k, v in schema.items() if str(v).lower() in ("string", "text", "str")]
                title_col = string_cols[0] if string_cols else (next(iter(schema.keys()), "id"))
            numeric_cols = [k for k, v in schema.items() if str(v).lower() in ("int", "integer", "float", "double", "numeric", "decimal")]
            value_col = numeric_cols[0] if numeric_cols else next(iter(schema.keys()), "id")
            gold_tasks.append((src_name, [
                ("Build gold", lambda: build_gold_content_rollups(src_name, title_col=title_col, value_col=value_col, agg="sum")),
            ]))
        # Reports tasks
        reports_tasks = [
            ("Create report views", lambda: create_report_views(conn)),
            ("Run reports SQL", lambda: run_reports_sql(conn)),
        ]
        # Print and run tasks
        print_main_task("Bronze")
        for src_name, subtasks in bronze_tasks:
            print(f"  {src_name}")
            for sub_name, fn in subtasks:
                print_subtask_status(sub_name, None)
                try:
                    fn()
                    print_subtask_status(sub_name, "ok")
                except Exception as e:
                    print_subtask_status(sub_name, "fail")
                    logger.error(f"Task failed: Bronze/{src_name}/{sub_name}: {e}")
        print_main_task("Silver")
        for src_name, subtasks in silver_tasks:
            print(f"  {src_name}")
            for sub_name, fn in subtasks:
                print_subtask_status(sub_name, None)
                try:
                    fn()
                    print_subtask_status(sub_name, "ok")
                except Exception as e:
                    print_subtask_status(sub_name, "fail")
                    logger.error(f"Task failed: Silver/{src_name}/{sub_name}: {e}")
        print_main_task("Gold")
        for src_name, subtasks in gold_tasks:
            print(f"  {src_name}")
            for sub_name, fn in subtasks:
                print_subtask_status(sub_name, None)
                try:
                    fn()
                    print_subtask_status(sub_name, "ok")
                except Exception as e:
                    print_subtask_status(sub_name, "fail")
                    logger.error(f"Task failed: Gold/{src_name}/{sub_name}: {e}")
        print_main_task("Reports")
        for sub_name, fn in reports_tasks:
            print_subtask_status(sub_name, None)
            try:
                fn()
                print_subtask_status(sub_name, "ok")
            except Exception as e:
                print_subtask_status(sub_name, "fail")
                logger.error(f"Task failed: Reports/{sub_name}: {e}")
        print("\nPipeline complete.")
        return
    # After building all sources, (re)create report views
    create_report_views(conn)
    # Generate CSV report exports automatically
    run_reports_sql(conn)


if __name__ == "__main__":
    try:
        cli()
    except KeyboardInterrupt:
        sys.exit(130)

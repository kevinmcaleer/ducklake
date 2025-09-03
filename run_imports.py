#!/usr/bin/env python3
import argparse
import os
import pathlib
import shlex
import subprocess
import sys
import time
import uuid
from datetime import date, datetime

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover
    duckdb = None

ROOT = pathlib.Path(__file__).resolve().parent
CFG = ROOT / "configs" / "sources.yml"
INTAKE = ROOT / "intake.py"
DB = ROOT / "duckdb" / "lake.duckdb"
LOCK = ROOT / ".ducklake.lock"


def run(cmd: list[str], retries: int = 1, backoff: float = 2.0) -> int:
    print("+", " ".join(shlex.quote(c) for c in cmd))
    rc = 1
    for attempt in range(1, max(1, retries) + 1):
        rc = subprocess.call(cmd, cwd=str(ROOT))
        if rc == 0:
            return 0
        if attempt < retries:
            sleep_s = backoff * (2 ** (attempt - 1))
            print(f"WARN: rc={rc}; retry {attempt}/{retries-1} in {sleep_s:.1f}s")
            time.sleep(sleep_s)
    return rc


def db_exec(sql: str, params: list | None = None):
    if duckdb is None:
        return
    con = duckdb.connect(str(DB))
    try:
        return con.execute(sql, params or [])
    finally:
        con.close()


def ensure_run_tables():
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT,
          started_ts TIMESTAMP,
          ended_ts TIMESTAMP,
          dt DATE,
          status TEXT,
          params JSON
        )
        """
    )
    db_exec(
        """
        CREATE TABLE IF NOT EXISTS run_steps (
          run_id TEXT,
          ts TIMESTAMP,
          source TEXT,
          step TEXT,
          status TEXT,
          message TEXT
        )
        """
    )


def record_run_start(dt_str: str, params: dict) -> str:
    import json
    ensure_run_tables()
    run_id = str(uuid.uuid4())
    db_exec(
        "INSERT INTO runs VALUES (?, now(), NULL, ?, ?, ?)",
        [run_id, dt_str, "RUNNING", json.dumps(params)],
    )
    return run_id


def record_step(run_id: str, source: str, step: str, status: str, message: str = ""):
    db_exec(
        "INSERT INTO run_steps VALUES (?, now(), ?, ?, ?, ?)",
        [run_id, source, step, status, message[:1000]],
    )


def record_run_end(run_id: str, status: str):
    db_exec(
        "UPDATE runs SET ended_ts = now(), status = ? WHERE run_id = ?",
        [status, run_id],
    )


def acquire_lock(ttl_seconds: int = 7200) -> bool:
    if LOCK.exists():
        try:
            age = time.time() - LOCK.stat().st_mtime
            if age < ttl_seconds:
                print(f"Another run is active (lock age {age:.0f}s). Exiting.")
                return False
        except FileNotFoundError:
            pass
    try:
        LOCK.write_text(datetime.utcnow().isoformat())
        return True
    except OSError as e:
        print(f"WARN: cannot write lock file: {e}")
        return False


def release_lock():
    try:
        LOCK.unlink(missing_ok=True)
    except OSError:
        pass


def main() -> int:
    ap = argparse.ArgumentParser(description="Run ducklake imports from configs/sources.yml and build silver/gold")
    ap.add_argument("--dt", default=date.today().isoformat(), help="Partition date YYYY-MM-DD (default: today)")
    ap.add_argument("--insecure", action="store_true", help="Allow insecure SSL for ingest-url")
    ap.add_argument("--only", default=None, help="Comma-separated list of sources to process (default: all)")
    ap.add_argument("--no-ingest", action="store_true", help="Skip ingestion; only build silver/gold")
    ap.add_argument("--retries", type=int, default=2, help="Retries per step (default: 2)")
    ap.add_argument("--reports", action="store_true", help="Generate CSV reports via reports.sql at the end")
    # convenience override for search_logs path
    ap.add_argument("--search-logs-file", default=None, help="Local path to search logs file for source 'search_logs'")
    args = ap.parse_args()

    if yaml is None:
        print("ERROR: PyYAML not installed. Activate venv and pip install pyyaml.", file=sys.stderr)
        return 2
    if not CFG.exists():
        print(f"ERROR: {CFG} not found", file=sys.stderr)
        return 2

    if not acquire_lock():
        return 3

    with CFG.open("r", encoding="utf-8") as f:
        sources = yaml.safe_load(f) or {}
    if not isinstance(sources, dict):
        print("ERROR: Invalid sources.yml format", file=sys.stderr)
        release_lock()
        return 2

    only = None
    if args.only:
        only = {s.strip() for s in args.only.split(",") if s.strip()}

    py = sys.executable
    dt = args.dt
    insecure_flag = ["--insecure"] if args.insecure else []
    run_id = record_run_start(dt, {"dt": dt, "only": args.only, "insecure": args.insecure})

    overall_status = "SUCCESS"
    try:
        for name, cfg in sources.items():
            if only and name not in only:
                continue
            fmt = str((cfg or {}).get("format") or "").lower()
            url = (cfg or {}).get("url")
            expect_schema = (cfg or {}).get("expect_schema") or {}

            # Ingest
            if not args.no_ingest:
                step = "ingest"
                if isinstance(url, str) and url.startswith(("http://", "https://")):
                    cmd = [py, str(INTAKE), "ingest-url", name, url, "--format", fmt, "--dt", dt, *insecure_flag]
                    rc = run(cmd, retries=args.retries)
                elif isinstance(url, str) and url.startswith("file://"):
                    local_path = url.replace("file://", "", 1)
                    cmd = [py, str(INTAKE), "ingest-file", name, local_path, "--format", fmt, "--dt", dt]
                    rc = run(cmd, retries=args.retries)
                elif isinstance(url, str) and url.startswith("smb://"):
                    override = None
                    if name == "search_logs" and args.search_logs_file:
                        override = args.search_logs_file
                    if not override:
                        override = os.getenv(f"SOURCE_{name.upper()}_FILE") or os.getenv("SEARCH_LOGS_FILE")
                    if override:
                        cmd = [py, str(INTAKE), "ingest-file", name, override, "--format", fmt, "--dt", dt]
                        rc = run(cmd, retries=args.retries)
                    else:
                        print(f"INFO: Skipping ingestion for {name}: provide --search-logs-file or env SEARCH_LOGS_FILE")
                        rc = 0
                else:
                    override = os.getenv(f"SOURCE_{name.upper()}_FILE")
                    if override:
                        cmd = [py, str(INTAKE), "ingest-file", name, override, "--format", fmt, "--dt", dt]
                        rc = run(cmd, retries=args.retries)
                    else:
                        print(f"INFO: No ingestion rule for {name} (url={url!r}), skipping ingest")
                        rc = 0
                record_step(run_id, name, step, "SUCCESS" if rc == 0 else "FAIL", "rc=" + str(rc))
                if rc != 0:
                    overall_status = "PARTIAL_FAIL"

            # Silver
            step = "silver"
            rc = run([py, str(INTAKE), "silver", name], retries=args.retries)
            record_step(run_id, name, step, "SUCCESS" if rc == 0 else "FAIL", "rc=" + str(rc))
            if rc != 0:
                overall_status = "PARTIAL_FAIL"
                continue

            # Gold (choose sensible defaults)
            step = "gold"
            title_col = None
            if "query" in expect_schema:
                title_col = "query"
            elif "url" in expect_schema:
                title_col = "url"
            if title_col:
                rc = run([py, str(INTAKE), "gold", name, "--title-col", title_col, "--agg", "count"], retries=args.retries)
            else:
                rc = run([py, str(INTAKE), "gold", name], retries=args.retries)
            record_step(run_id, name, step, "SUCCESS" if rc == 0 else "FAIL", "rc=" + str(rc))
            if rc != 0:
                overall_status = "PARTIAL_FAIL"

        if args.reports:
            step = "reports"
            reports_sql = ROOT / "reports.sql"
            if reports_sql.exists():
                rc = run(["duckdb", str(DB), "-c", ".read reports.sql"], retries=1)
                record_step(run_id, "_system", step, "SUCCESS" if rc == 0 else "FAIL", "rc=" + str(rc))
                if rc != 0:
                    overall_status = "PARTIAL_FAIL"
            else:
                record_step(run_id, "_system", step, "SKIPPED", "reports.sql not found")

    finally:
        record_run_end(run_id, overall_status)
        release_lock()

    return 0 if overall_status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import os
import pathlib
import shlex
import subprocess
import sys
from datetime import date

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

ROOT = pathlib.Path(__file__).resolve().parent
CFG = ROOT / "configs" / "sources.yml"
INTAKE = ROOT / "intake.py"


def run(cmd: list[str]) -> int:
    print("+", " ".join(shlex.quote(c) for c in cmd))
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(description="Run ducklake imports from configs/sources.yml and build silver/gold")
    ap.add_argument("--dt", default=date.today().isoformat(), help="Partition date YYYY-MM-DD (default: today)")
    ap.add_argument("--insecure", action="store_true", help="Allow insecure SSL for ingest-url")
    ap.add_argument("--only", default=None, help="Comma-separated list of sources to process (default: all)")
    ap.add_argument("--no-ingest", action="store_true", help="Skip ingestion; only build silver/gold")
    # convenience override for search_logs path
    ap.add_argument("--search-logs-file", default=None, help="Local path to search logs file for source 'search_logs'")
    args = ap.parse_args()

    if yaml is None:
        print("ERROR: PyYAML not installed. Activate venv and pip install pyyaml.", file=sys.stderr)
        return 2
    if not CFG.exists():
        print(f"ERROR: {CFG} not found", file=sys.stderr)
        return 2

    with CFG.open("r", encoding="utf-8") as f:
        sources = yaml.safe_load(f) or {}
    if not isinstance(sources, dict):
        print("ERROR: Invalid sources.yml format", file=sys.stderr)
        return 2

    only = None
    if args.only:
        only = {s.strip() for s in args.only.split(",") if s.strip()}

    py = sys.executable
    dt = args.dt
    insecure_flag = ["--insecure"] if args.insecure else []

    for name, cfg in sources.items():
        if only and name not in only:
            continue
        fmt = str((cfg or {}).get("format") or "").lower()
        url = (cfg or {}).get("url")
        expect_schema = (cfg or {}).get("expect_schema") or {}

        # Ingest
        if not args.no_ingest:
            if isinstance(url, str) and url.startswith(("http://", "https://")):
                cmd = [py, str(INTAKE), "ingest-url", name, url, "--format", fmt, "--dt", dt, *insecure_flag]
                rc = run(cmd)
                if rc != 0:
                    print(f"WARN: ingest-url failed for {name} (rc={rc}), continuing", file=sys.stderr)
            elif isinstance(url, str) and url.startswith("file://"):
                local_path = url.replace("file://", "", 1)
                cmd = [py, str(INTAKE), "ingest-file", name, local_path, "--format", fmt, "--dt", dt]
                rc = run(cmd)
                if rc != 0:
                    print(f"WARN: ingest-file failed for {name} path={local_path} (rc={rc})", file=sys.stderr)
            elif isinstance(url, str) and url.startswith("smb://"):
                # require a local override path via CLI or env
                override = None
                if name == "search_logs" and args.search_logs_file:
                    override = args.search_logs_file
                if not override:
                    override = os.getenv(f"SOURCE_{name.upper()}_FILE") or os.getenv("SEARCH_LOGS_FILE")
                if override:
                    cmd = [py, str(INTAKE), "ingest-file", name, override, "--format", fmt, "--dt", dt]
                    rc = run(cmd)
                    if rc != 0:
                        print(f"WARN: ingest-file failed for {name} path={override} (rc={rc})", file=sys.stderr)
                else:
                    print(f"INFO: Skipping ingestion for {name}: provide a local path via --search-logs-file or env SEARCH_LOGS_FILE", file=sys.stderr)
            else:
                # If url absent, also look for SOURCE_<NAME>_FILE env
                override = os.getenv(f"SOURCE_{name.upper()}_FILE")
                if override:
                    cmd = [py, str(INTAKE), "ingest-file", name, override, "--format", fmt, "--dt", dt]
                    rc = run(cmd)
                    if rc != 0:
                        print(f"WARN: ingest-file failed for {name} path={override} (rc={rc})", file=sys.stderr)
                else:
                    print(f"INFO: No ingestion rule for {name} (url={url!r}), skipping ingest", file=sys.stderr)

        # Silver
        rc = run([py, str(INTAKE), "silver", name])
        if rc != 0:
            print(f"ERROR: silver failed for {name} (rc={rc})", file=sys.stderr)
            continue

        # Gold (choose sensible defaults)
        title_col = None
        if "query" in expect_schema:
            title_col = "query"
        elif "url" in expect_schema:
            title_col = "url"
        if title_col:
            rc = run([py, str(INTAKE), "gold", name, "--title-col", title_col, "--agg", "count"])
            if rc != 0:
                print(f"WARN: gold failed for {name} (rc={rc})", file=sys.stderr)
        else:
            # fallback to generic refresh gold if no obvious title_col
            rc = run([py, str(INTAKE), "gold", name])
            if rc != 0:
                print(f"WARN: gold (generic) failed for {name} (rc={rc})", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

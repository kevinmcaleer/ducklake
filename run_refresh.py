#!/usr/bin/env python
"""Simple command-line interface for the Ducklake simple pipeline.

Usage examples:
  # Basic refresh (ingest new raw CSVs -> enrich -> aggregates -> reports)
  python run_refresh.py --db contentlake.ducklake

  # Refresh with custom root (if running outside repo root)
  python run_refresh.py --db contentlake.ducklake --root .

  # Incremental snapshot ingest before refresh
  python run_refresh.py snapshot \
      --db contentlake.ducklake \
      --source search_logs \
      --snapshot data/snapshots/search_logs_snapshot.csv \
      --time-col timestamp

Exit codes:
  0 success
  1 unexpected failure
"""
from __future__ import annotations
import argparse, sys, pathlib, json, duckdb, shutil, datetime, os
from typing import Optional
from ducklake_core.simple_pipeline import (
    simple_refresh,
    incremental_snapshot_ingest,
    ensure_core_tables,
    LAKE_DIR,
    SOURCES,
    RAW_DIR,
    REPORTS_DIR,
)
from ducklake_core.fetch_raw import (
    fetch_page_count_recent,
    fetch_page_count_postgres,
    fetch_search_logs_snapshot,
    fetch_page_count_all,
    fetch_search_logs_file,
    fetch_search_logs_postgres,
)
from ducklake_core.export_reports import export_reports_command


# ------------------------
# Helper utilities
# ------------------------
def print_result(obj: dict, use_json: bool):
    """Uniform output printer. If timings present, format them nicely in text mode.
    Always stable: callers pass a dict.
    """
    if use_json:
        print(json.dumps(obj, indent=2, default=str))
        return
    if 'timings' in obj:
        base = {k: v for k, v in obj.items() if k != 'timings'}
        for k, v in base.items():
            if isinstance(v, dict):
                print(f"{k}:")
                for sk, sv in v.items():
                    print(f"  {sk}: {sv}")
            else:
                print(f"{k}: {v}")
        print("timings:")
        for tk, tv in obj['timings'].items():
            print(f"  {tk}: {tv}")
    else:
        print(obj)


def maybe_auto_fetch_page_count(args) -> dict | None:
    """Perform optional auto-fetch for page_count based on --auto-fetch-days.
    Returns a result dict or None. Never raises.
    """
    afd = getattr(args, 'auto_fetch_days', None)
    if not afd:
        return None
    try:
        if str(afd).lower() == 'all':
            print("[auto-fetch] fetching ALL page_count history before refresh")
            return fetch_page_count_all(overwrite=False)
        days = int(afd)
        if days <= 0:
            return None
        print(f"[auto-fetch] fetching last {days} day(s) of page_count before refresh")
        return fetch_page_count_recent(days, overwrite=False)
    except Exception as e:
        print(f"[auto-fetch] warning: failed auto fetch ({e})", file=sys.stderr)
        return None


def _connect(path: str):
    return duckdb.connect(path)


def _force_cleanup(con: duckdb.DuckDBPyConnection, args: argparse.Namespace):
    ensure_core_tables(con)
    targets = args.force_sources if args.force_sources else SOURCES
    print(f"[force] targeting sources: {', '.join(targets)}")
    for src in targets:
        try:
            con.execute("DELETE FROM processed_files WHERE source=?", [src])
            print(f"[force] cleared processed_files entries for source={src}")
        except Exception as e:  # table might not exist yet
            print(f"[force] warn could not clear processed_files for {src}: {e}")
        if args.rebuild_lake:
            base = LAKE_DIR / src
            if base.exists():
                try:
                    shutil.rmtree(base)
                    print(f"[force] removed lake directory {base}")
                except Exception as e:
                    print(f"[force] failed removing {base}: {e}")
            base.mkdir(parents=True, exist_ok=True)


def _reimport_last_days(con: duckdb.DuckDBPyConnection, args: argparse.Namespace):
    days = args.reimport_last_days
    if not days or days <= 0:
        return
    ensure_core_tables(con)
    cutoff = (datetime.date.today() - datetime.timedelta(days=days-1))  # include today as day 1
    targets = args.force_sources if args.force_sources else SOURCES
    print(f"[reimport] last {days} days starting {cutoff} for sources: {', '.join(targets)}")
    for src in targets:
        # Remove processed_files records for those dates if we can infer dt from path (dt=YYYY-MM-DD in path)
        # Simpler: delete all rows for source where dt >= cutoff
        try:
            con.execute("DELETE FROM processed_files WHERE source=? AND dt >= ?", [src, cutoff])
            print(f"[reimport] cleared processed_files >= {cutoff} for source={src}")
        except Exception as e:
            print(f"[reimport] warn could not clear processed_files for {src}: {e}")
        # Delete lake parquet partitions for affected days
        base = LAKE_DIR / src
        if base.exists():
            # Patterns: dt=YYYY-MM-DD.parquet OR directories dt=YYYY-MM-DD/...
            for p in base.glob('dt=*'):
                try:
                    name = p.name.split('=')[1]
                except Exception:
                    continue
                try:
                    p_dt = datetime.date.fromisoformat(name.replace('.parquet','')) if '.parquet' in name else datetime.date.fromisoformat(name)
                except Exception:
                    continue
                if p_dt >= cutoff:
                    try:
                        if p.is_dir():
                            shutil.rmtree(p)
                        else:
                            p.unlink()
                        print(f"[reimport] removed partition {p}")
                    except Exception as e:
                        print(f"[reimport] failed removing partition {p}: {e}")


def cmd_refresh(args: argparse.Namespace) -> int:
    print("[progress] Starting refresh...", file=sys.stderr, flush=True)
    con = _connect(args.db)
    auto_fetch_result = maybe_auto_fetch_page_count(args)
    if args.force:
        _force_cleanup(con, args)
    if args.reimport_last_days:
        _reimport_last_days(con, args)
    print("[progress] Running pipeline refresh...", file=sys.stderr, flush=True)
    result = simple_refresh(con)
    # Always include auto_fetch key for stable schema
    result['auto_fetch'] = auto_fetch_result

    # Close connection before export to avoid lock conflicts
    con.close()

    # Auto-export to Metabase if --export-metabase flag is set
    export_result = None
    if args.export_metabase:
        try:
            print("[export] Exporting to Metabase-compatible format...")
            import subprocess
            export_script = pathlib.Path(__file__).parent / "export_for_metabase.sh"
            proc_result = subprocess.run([str(export_script)], capture_output=True, text=True)
            if proc_result.returncode == 0:
                print(proc_result.stdout)
                export_result = {"status": "success", "output": "reports.duckdb"}
            else:
                print(f"[export] Warning: Failed to export to Metabase format", file=sys.stderr)
                print(f"[export] stdout: {proc_result.stdout}", file=sys.stderr)
                print(f"[export] stderr: {proc_result.stderr}", file=sys.stderr)
                export_result = {"status": "failed", "error": proc_result.stderr}
        except subprocess.CalledProcessError as e:
            print(f"[export] Warning: Failed to export to Metabase format", file=sys.stderr)
            print(f"[export] Error output: {e.stderr}", file=sys.stderr)
            export_result = {"status": "failed", "error": str(e)}
        except Exception as e:
            print(f"[export] Warning: Failed to export to Metabase format: {e}", file=sys.stderr)
            export_result = {"status": "failed", "error": str(e)}

    result['metabase_export'] = export_result
    print_result(result, args.json)
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    snap_path = pathlib.Path(args.snapshot)
    if not snap_path.exists():
        msg = f"[snapshot] file not found: {args.snapshot}. Create it or provide the correct --snapshot path."
        payload = {"error": msg, "snapshot": str(snap_path)}
        print_result(payload, args.json)
        return 1
    con = _connect(args.db)
    snap_res = incremental_snapshot_ingest(
        con,
        args.source,
        args.snapshot,
        time_col=args.time_col,
        quality=not args.no_quality,
    )
    ref = None if args.no_refresh else simple_refresh(con)
    out = {"snapshot": snap_res, "refresh": ref, "auto_fetch": None}
    print_result(out, args.json)
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    db_path = pathlib.Path(args.db)
    # Close DB if exists by just not connecting; removing file first is fine if we haven't opened it.
    removed = {}
    # Remove DB file
    if db_path.exists():
        try:
            db_path.unlink()
            removed['db'] = True
            print(f"[reset] removed database file {db_path}")
        except Exception as e:
            print(f"[reset] failed removing db {db_path}: {e}")
            removed['db'] = False
    # Remove lake directory
    if LAKE_DIR.exists():
        try:
            shutil.rmtree(LAKE_DIR)
            print(f"[reset] removed lake directory {LAKE_DIR}")
            removed['lake'] = True
        except Exception as e:
            print(f"[reset] failed removing lake dir {LAKE_DIR}: {e}")
            removed['lake'] = False
    # Remove silver enrichment artifacts
    silver_dir = pathlib.Path(__file__).resolve().parent / 'silver'
    if silver_dir.exists():
        try:
            shutil.rmtree(silver_dir)
            print(f"[reset] removed silver directory {silver_dir}")
            removed['silver'] = True
        except Exception as e:
            print(f"[reset] failed removing silver dir {silver_dir}: {e}")
            removed['silver'] = False
    # Remove reports
    if REPORTS_DIR.exists():
        try:
            shutil.rmtree(REPORTS_DIR)
            print(f"[reset] removed reports directory {REPORTS_DIR}")
            removed['reports'] = True
        except Exception as e:
            print(f"[reset] failed removing reports dir {REPORTS_DIR}: {e}")
            removed['reports'] = False
    # Optionally raw data
    if args.include_raw and RAW_DIR.exists():
        try:
            shutil.rmtree(RAW_DIR)
            print(f"[reset] removed raw directory {RAW_DIR}")
            removed['raw'] = True
        except Exception as e:
            print(f"[reset] failed removing raw dir {RAW_DIR}: {e}")
            removed['raw'] = False
    # Summary JSON
    summary = {'reset': removed, 'ts': datetime.datetime.utcnow().isoformat() + 'Z', 'auto_fetch': None}
    print_result(summary, True)  # reset output is always JSON-like
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    print("[progress] Starting fetch...", file=sys.stderr, flush=True)
    results = {}
    # Default behavior: if --sources omitted, fetch both sources
    sources = args.sources if args.sources is not None else ['page_count', 'search_logs']
    if 'page_count' in sources or 'all' in sources:
        # Use Postgres by default if PAGE_COUNT_DATABASE_URL is set or DB_HOST is available
        use_postgres = getattr(args, 'page_count_postgres', False) or (
            (os.getenv('PAGE_COUNT_DATABASE_URL') or os.getenv('DB_HOST')) and not args.page_count_url
        )
        if use_postgres:
            results['page_count'] = fetch_page_count_postgres(
                overwrite=args.overwrite,
                days=args.days,
            )
        else:
            results['page_count'] = fetch_page_count_recent(
                args.days,
                overwrite=args.overwrite,
                base_url=args.page_count_url,
                api_key=args.page_count_key,
                fallback_dir=pathlib.Path(args.fallback_dir) if args.fallback_dir else None,
            )
    if 'search_logs' in sources or 'all' in sources:
        # Use Postgres by default if DATABASE_URL is set, otherwise use API
        use_postgres = getattr(args, 'search_logs_postgres', False) or (os.getenv('DATABASE_URL') and not args.search_logs_url)
        if use_postgres:
            results['search_logs'] = fetch_search_logs_postgres(
                overwrite=args.overwrite,
                days=args.days,
            )
        else:
            results['search_logs'] = fetch_search_logs_snapshot(
                snapshot_url=args.search_logs_url,
                overwrite=args.overwrite,
                base_url=args.search_logs_url,
                api_key=args.search_logs_key,
            )
    if args.refresh:
        print("[progress] Running pipeline refresh...", file=sys.stderr, flush=True)
    ref = simple_refresh(_connect(args.db)) if args.refresh else None
    payload = {'fetched': results, 'refresh': ref, 'auto_fetch': None}
    print_result(payload, args.json)
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    """Full historical backfill for page_count (and optionally search logs snapshot) then refresh.

    Page count: uses Postgres by default if available, otherwise attempts API full export or local archives.
    Search logs: uses Postgres by default if available, otherwise uses provided snapshot URL or file.
    """
    print("[progress] Starting backfill...", file=sys.stderr, flush=True)
    results = {}

    # Use Postgres for page_count if available
    use_postgres = getattr(args, 'page_count_postgres', False) or (
        (os.getenv('PAGE_COUNT_DATABASE_URL') or os.getenv('DB_HOST')) and not args.page_count_url
    )
    if use_postgres:
        results['page_count'] = fetch_page_count_postgres(overwrite=args.overwrite, days=None)  # None = all data
    else:
        results['page_count'] = fetch_page_count_all(overwrite=args.overwrite, base_url=args.page_count_url, api_key=args.page_count_key, fallback_dir=pathlib.Path(args.fallback_dir) if args.fallback_dir else None)

    # Determine which search logs source to use (priority: postgres > url > file)
    use_postgres = getattr(args, 'search_logs_postgres', False) or (os.getenv('DATABASE_URL') and not args.search_logs_url and not args.search_logs_file)

    if use_postgres:
        results['search_logs'] = fetch_search_logs_postgres(overwrite=args.overwrite, days=None)  # None = all data
    elif args.search_logs_url:
        results['search_logs'] = fetch_search_logs_snapshot(snapshot_url=args.search_logs_url, overwrite=args.overwrite, base_url=args.search_logs_url, api_key=args.search_logs_key)
    elif args.search_logs_file:
        results['search_logs'] = fetch_search_logs_file(args.search_logs_file, overwrite=args.overwrite)
    if not args.no_refresh:
        print("[progress] Running pipeline refresh...", file=sys.stderr, flush=True)
        con = _connect(args.db)
        ref = simple_refresh(con)
    else:
        ref = None
    payload = {'backfill': results, 'refresh': ref, 'auto_fetch': None}
    print_result(payload, args.json)
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export all tables and views to a reporting database for Metabase."""
    result = export_reports_command(
        args.db,
        output_path=args.output,
        json_output=args.json
    )
    if args.json:
        print_result(result, True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ducklake simple pipeline CLI")
    p.add_argument("--db", default="contentlake.ducklake", help="DuckDB database file path")
    p.add_argument("--root", default=".", help="Project root (unused placeholder for future)")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    p.add_argument("--auto-fetch-days", help="Before refresh: fetch page_count recent N days (or 'all') automatically", default=None)
    sub = p.add_subparsers(dest="subcmd")

    # Global force / reimport options (apply to refresh only)
    p.add_argument("--force", action="store_true", help="Force re-ingest by clearing processed_files (all sources or subset)")
    p.add_argument("--force-sources", nargs="*", default=None, help="Subset of sources to force (default: all)")
    p.add_argument("--rebuild-lake", action="store_true", help="When forcing, delete lake parquet partitions for affected sources before ingest")
    p.add_argument("--reimport-last-days", type=int, default=0, help="Re-import only the last N days (clears processed_files rows and lake partitions for those days)")
    p.add_argument("--export-metabase", action="store_true", help="After refresh, automatically export to reports.duckdb for Metabase (DuckDB 1.3.1 format)")

    # Subcommand: snapshot
    sp = sub.add_parser("snapshot", help="Run incremental snapshot ingest then refresh")
    sp.add_argument("--source", required=True, help="Source name (e.g. search_logs)")
    sp.add_argument("--snapshot", required=True, help="Path to cumulative snapshot CSV")
    sp.add_argument("--time-col", default="timestamp", help="Timestamp column in snapshot")
    sp.add_argument("--no-quality", action="store_true", help="Disable query / row quality filters")
    sp.add_argument("--no-refresh", action="store_true", help="Skip running simple_refresh after ingest")
    sp.add_argument("--json", action="store_true", help="JSON output (overrides top-level flag)")
    sp.set_defaults(func=cmd_snapshot)

    # Subcommand: reset
    rp = sub.add_parser("reset", help="Remove DB file, lake, silver, reports (optionally raw) for a clean slate")
    rp.add_argument("--include-raw", action="store_true", help="Also delete data/raw directory")
    rp.set_defaults(func=cmd_reset)

    # Subcommand: fetch (download raw then optional refresh)
    fp = sub.add_parser("fetch", help="Fetch raw source data (recent page_count + search logs snapshot) then optional refresh (defaults to all sources)")
    fp.add_argument("--sources", nargs="+", default=None, help="Subset of sources to fetch (default: both page_count and search_logs). Accepts: page_count search_logs all")
    fp.add_argument("--days", type=int, default=1, help="Recent days to fetch for page_count")
    fp.add_argument("--page-count-url", help="Override PAGE_COUNT_API_URL")
    fp.add_argument("--page-count-key", help="API key for page_count endpoint")
    fp.add_argument("--page-count-postgres", action="store_true", help="Fetch page_count from Postgres database (uses PAGE_COUNT_DATABASE_URL or DB_HOST from .env)")
    fp.add_argument("--search-logs-url", help="Override SEARCH_LOGS_API_URL / snapshot URL")
    fp.add_argument("--search-logs-key", help="API key for search_logs endpoint")
    fp.add_argument("--search-logs-postgres", action="store_true", help="Fetch search logs from Postgres database (uses DATABASE_URL from .env)")
    fp.add_argument("--fallback-dir", help="Fallback directory of local JSONL files (all-visits-*.jsonl)")
    fp.add_argument("--overwrite", action="store_true", help="Overwrite existing raw CSV partitions for fetched days")
    fp.add_argument("--refresh", action="store_true", help="Run pipeline refresh after fetch")
    fp.add_argument("--json", action="store_true", help="JSON output (overrides top-level flag)")
    fp.set_defaults(func=cmd_fetch)

    # Subcommand: backfill (full historical for page_count + optional search logs snapshot)
    bp = sub.add_parser("backfill", help="Full historical backfill for page_count (API all=1 or all local archives) and optional search logs snapshot")
    bp.add_argument("--page-count-url", help="API base URL for page_count full export (will append all=1 if not present)")
    bp.add_argument("--page-count-key", help="API key for page_count endpoint")
    bp.add_argument("--page-count-postgres", action="store_true", help="Fetch page_count from Postgres database (uses PAGE_COUNT_DATABASE_URL or DB_HOST from .env)")
    bp.add_argument("--search-logs-url", help="Full snapshot URL for search logs (JSON or JSONL)")
    bp.add_argument("--search-logs-file", help="Local search logs file path (e.g. /Volumes/Search\\ Logs/Search-Logs.log)")
    bp.add_argument("--search-logs-postgres", action="store_true", help="Fetch search logs from Postgres database (uses DATABASE_URL from .env)")
    bp.add_argument("--search-logs-key", help="API key for search_logs endpoint")
    bp.add_argument("--fallback-dir", help="Fallback directory of local JSONL files (all-visits-*.jsonl)")
    bp.add_argument("--overwrite", action="store_true", help="Overwrite existing per-day raw CSV partitions")
    bp.add_argument("--no-refresh", action="store_true", help="Skip running simple_refresh after backfill")
    bp.add_argument("--json", action="store_true", help="JSON output (overrides top-level flag)")
    bp.set_defaults(func=cmd_backfill)

    # Subcommand: export (export tables and views to reporting database)
    ep = sub.add_parser("export", help="Export all tables and views to a reporting database for Metabase")
    ep.add_argument("--output", default="reports.duckdb", help="Output database path (default: reports.duckdb)")
    ep.add_argument("--json", action="store_true", help="JSON output (overrides top-level flag)")
    ep.set_defaults(func=cmd_export)

    p.set_defaults(func=cmd_refresh)
    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 1
    except Exception as e:  # pragma: no cover
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

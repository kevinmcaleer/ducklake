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
import argparse, sys, pathlib, json, duckdb, shutil, datetime
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
from ducklake_core.fetch_raw import fetch_page_count_recent, fetch_search_logs_snapshot, fetch_page_count_all, fetch_search_logs_file


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
    con = _connect(args.db)
    if args.force:
        _force_cleanup(con, args)
    if args.reimport_last_days:
        _reimport_last_days(con, args)
    result = simple_refresh(con)
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print("Simple refresh complete:")
        for k, v in result.items():
            if k == 'timings':
                print("  timings:")
                for tk, tv in v.items():
                    print(f"    {tk}: {tv}")
            else:
                print(f"  {k}: {v}")
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    con = _connect(args.db)
    snap_res = incremental_snapshot_ingest(
        con,
        args.source,
        args.snapshot,
        time_col=args.time_col,
        quality=not args.no_quality,
    )
    if not args.no_refresh:
        ref = simple_refresh(con)
    else:
        ref = None
    out = {"snapshot": snap_res, "refresh": ref}
    print(json.dumps(out, indent=2, default=str) if args.json else out)
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
    summary = {'reset': removed, 'ts': datetime.datetime.utcnow().isoformat() + 'Z'}
    print(json.dumps(summary, indent=2))
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    results = {}
    if 'page_count' in args.sources or 'all' in args.sources:
        results['page_count'] = fetch_page_count_recent(args.days, overwrite=args.overwrite, base_url=args.page_count_url, api_key=args.page_count_key, fallback_dir=pathlib.Path(args.fallback_dir) if args.fallback_dir else None)
    if 'search_logs' in args.sources or 'all' in args.sources:
        results['search_logs'] = fetch_search_logs_snapshot(snapshot_url=args.search_logs_url, overwrite=args.overwrite, base_url=args.search_logs_url, api_key=args.search_logs_key)
    if args.refresh:
        # Run a refresh after fetching
        con = _connect(args.db)
        ref = simple_refresh(con)
    else:
        ref = None
    payload = {'fetched': results, 'refresh': ref}
    print(json.dumps(payload, indent=2, default=str) if args.json else payload)
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    """Full historical backfill for page_count (and optionally search logs snapshot) then refresh.

    Page count: attempts API full export (all=1) else consumes every local JSONL archive.
    Search logs: if --search-logs-url provided we treat it as a full snapshot and ingest per-day.
    """
    results = {}
    results['page_count'] = fetch_page_count_all(overwrite=args.overwrite, base_url=args.page_count_url, api_key=args.page_count_key, fallback_dir=pathlib.Path(args.fallback_dir) if args.fallback_dir else None)
    if args.search_logs_url:
        results['search_logs'] = fetch_search_logs_snapshot(snapshot_url=args.search_logs_url, overwrite=args.overwrite, base_url=args.search_logs_url, api_key=args.search_logs_key)
    elif args.search_logs_file:
        results['search_logs'] = fetch_search_logs_file(args.search_logs_file, overwrite=args.overwrite)
    if not args.no_refresh:
        con = _connect(args.db)
        ref = simple_refresh(con)
    else:
        ref = None
    payload = {'backfill': results, 'refresh': ref}
    print(json.dumps(payload, indent=2, default=str) if args.json else payload)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ducklake simple pipeline CLI")
    p.add_argument("--db", default="contentlake.ducklake", help="DuckDB database file path")
    p.add_argument("--root", default=".", help="Project root (unused placeholder for future)")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    sub = p.add_subparsers(dest="subcmd")

    # Global force / reimport options (apply to refresh only)
    p.add_argument("--force", action="store_true", help="Force re-ingest by clearing processed_files (all sources or subset)")
    p.add_argument("--force-sources", nargs="*", default=None, help="Subset of sources to force (default: all)")
    p.add_argument("--rebuild-lake", action="store_true", help="When forcing, delete lake parquet partitions for affected sources before ingest")
    p.add_argument("--reimport-last-days", type=int, default=0, help="Re-import only the last N days (clears processed_files rows and lake partitions for those days)")

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
    fp = sub.add_parser("fetch", help="Fetch raw source data (recent/page_count, snapshot search logs) then optional refresh")
    fp.add_argument("--sources", nargs="+", default=["all"], help="Sources to fetch: page_count search_logs all")
    fp.add_argument("--days", type=int, default=1, help="Recent days to fetch for page_count")
    fp.add_argument("--page-count-url", help="Override PAGE_COUNT_API_URL")
    fp.add_argument("--page-count-key", help="API key for page_count endpoint")
    fp.add_argument("--search-logs-url", help="Override SEARCH_LOGS_API_URL / snapshot URL")
    fp.add_argument("--search-logs-key", help="API key for search_logs endpoint")
    fp.add_argument("--fallback-dir", help="Fallback directory of local JSONL files (all-visits-*.jsonl)")
    fp.add_argument("--overwrite", action="store_true", help="Overwrite existing raw CSV partitions for fetched days")
    fp.add_argument("--refresh", action="store_true", help="Run pipeline refresh after fetch")
    fp.add_argument("--json", action="store_true", help="JSON output (overrides top-level flag)")
    fp.set_defaults(func=cmd_fetch)

    # Subcommand: backfill (full historical for page_count + optional search logs snapshot)
    bp = sub.add_parser("backfill", help="Full historical backfill for page_count (API all=1 or all local archives) and optional search logs snapshot")
    bp.add_argument("--page-count-url", help="API base URL for page_count full export (will append all=1 if not present)")
    bp.add_argument("--page-count-key", help="API key for page_count endpoint")
    bp.add_argument("--search-logs-url", help="Full snapshot URL for search logs (JSON or JSONL)")
    bp.add_argument("--search-logs-file", help="Local search logs file path (e.g. /Volumes/Search\\ Logs/Search-Logs.log)")
    bp.add_argument("--search-logs-key", help="API key for search_logs endpoint")
    bp.add_argument("--fallback-dir", help="Fallback directory of local JSONL files (all-visits-*.jsonl)")
    bp.add_argument("--overwrite", action="store_true", help="Overwrite existing per-day raw CSV partitions")
    bp.add_argument("--no-refresh", action="store_true", help="Skip running simple_refresh after backfill")
    bp.add_argument("--json", action="store_true", help="JSON output (overrides top-level flag)")
    bp.set_defaults(func=cmd_backfill)

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

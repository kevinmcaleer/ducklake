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
import argparse, sys, pathlib, json, duckdb, shutil
from typing import Optional
from ducklake_core.simple_pipeline import (
    simple_refresh,
    incremental_snapshot_ingest,
    ensure_core_tables,
    LAKE_DIR,
    SOURCES,
)


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


def cmd_refresh(args: argparse.Namespace) -> int:
    con = _connect(args.db)
    if args.force:
        _force_cleanup(con, args)
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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Ducklake simple pipeline CLI")
    p.add_argument("--db", default="contentlake.ducklake", help="DuckDB database file path")
    p.add_argument("--root", default=".", help="Project root (unused placeholder for future)")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    sub = p.add_subparsers(dest="subcmd")

    # Global force options (apply to refresh only)
    p.add_argument("--force", action="store_true", help="Force re-ingest by clearing processed_files (all sources or subset)")
    p.add_argument("--force-sources", nargs="*", default=None, help="Subset of sources to force (default: all)")
    p.add_argument("--rebuild-lake", action="store_true", help="When forcing, delete lake parquet partitions for affected sources before ingest")

    # Subcommand: snapshot
    sp = sub.add_parser("snapshot", help="Run incremental snapshot ingest then refresh")
    sp.add_argument("--source", required=True, help="Source name (e.g. search_logs)")
    sp.add_argument("--snapshot", required=True, help="Path to cumulative snapshot CSV")
    sp.add_argument("--time-col", default="timestamp", help="Timestamp column in snapshot")
    sp.add_argument("--no-quality", action="store_true", help="Disable query / row quality filters")
    sp.add_argument("--no-refresh", action="store_true", help="Skip running simple_refresh after ingest")
    sp.add_argument("--json", action="store_true", help="JSON output (overrides top-level flag)")
    sp.set_defaults(func=cmd_snapshot)

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

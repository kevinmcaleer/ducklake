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
import argparse, sys, pathlib, json, duckdb
from typing import Optional
from ducklake_core.simple_pipeline import (
    simple_refresh,
    incremental_snapshot_ingest,
)


def _connect(path: str):
    return duckdb.connect(path)


def cmd_refresh(args: argparse.Namespace) -> int:
    con = _connect(args.db)
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

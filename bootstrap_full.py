#!/usr/bin/env python
"""End-to-end bootstrap script:

1. Full historical page_count fetch from API (all=1) OR fallback archives.
2. Optional ingest of local search logs file.
3. Pipeline refresh (ingest -> enrich -> aggregates -> reports).

Environment / Arguments (simple flags for now):
  PAGE_COUNT_URL (env) or --page-count-url
  SEARCH_LOGS_FILE (env) or --search-logs-file
  --db (default contentlake.ducklake)
  --fallback-dir (for local all-visits-*.jsonl archives if API missing)
  --overwrite (rewrite raw per-day CSVs)
  --no-search (skip search logs ingest even if file provided)

Examples:
  python bootstrap_full.py --page-count-url http://page_count.kevsrobots.com/all-visits \
      --search-logs-file '/Volumes/Search Logs/Search-Logs.log'

  python bootstrap_full.py --fallback-dir _tmp_downloads
"""
from __future__ import annotations
import argparse, os, json, pathlib, duckdb, sys
from ducklake_core.fetch_raw import fetch_page_count_all, fetch_search_logs_file, fetch_search_logs_snapshot
from ducklake_core.simple_pipeline import simple_refresh, ensure_core_tables


def parse_args():
    p = argparse.ArgumentParser(description="Full bootstrap: fetch all raw + refresh")
    p.add_argument('--db', default='contentlake.ducklake')
    p.add_argument('--page-count-url', default=os.getenv('PAGE_COUNT_API_URL'))
    p.add_argument('--page-count-key', default=os.getenv('PAGE_COUNT_API_KEY'))
    p.add_argument('--search-logs-file', default=os.getenv('SEARCH_LOGS_FILE'))
    p.add_argument('--search-logs-url', default=os.getenv('SEARCH_LOGS_API_URL'))
    p.add_argument('--fallback-dir', help='Directory containing all-visits-*.jsonl archives when API unavailable')
    p.add_argument('--overwrite', action='store_true', help='Overwrite existing per-day raw CSV partitions')
    p.add_argument('--no-search', action='store_true', help='Skip search logs ingestion')
    p.add_argument('--json', action='store_true')
    return p.parse_args()


def main():
    args = parse_args()
    results = {}
    # 1. Page count full fetch
    results['page_count'] = fetch_page_count_all(overwrite=args.overwrite,
                                                base_url=args.page_count_url,
                                                api_key=args.page_count_key,
                                                fallback_dir=pathlib.Path(args.fallback_dir) if args.fallback_dir else None)
    # 2. Search logs (file preferred, else URL snapshot) unless skipped
    if not args.no_search:
        if args.search_logs_file:
            results['search_logs'] = fetch_search_logs_file(args.search_logs_file, overwrite=args.overwrite)
        elif args.search_logs_url:
            results['search_logs'] = fetch_search_logs_snapshot(snapshot_url=args.search_logs_url, overwrite=args.overwrite)
        else:
            results['search_logs'] = {'skipped': True, 'reason': 'no file or url provided'}
    else:
        results['search_logs'] = {'skipped': True, 'reason': '--no-search'}
    # 3. Refresh
    con = duckdb.connect(args.db)
    ensure_core_tables(con)
    refresh_res = simple_refresh(con)
    out = {'fetch': results, 'refresh': refresh_res}
    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

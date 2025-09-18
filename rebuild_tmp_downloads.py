#!/usr/bin/env python
"""Recreate _tmp_downloads all-visits-*.jsonl artifacts from raw page_count CSV partitions.

This is Step 1 of reconstructing the temporary downloads archive from scratch.

Usage:
  python rebuild_tmp_downloads.py --raw-dir data/raw/page_count --out-dir _tmp_downloads --overwrite

Behavior:
  - Scans raw page_count dt=YYYY-MM-DD directories for *.csv
  - Emits JSONL: all-visits-YYYY-MM-DD.jsonl with one JSON object per row
  - Preserves columns; ensures a 'timestamp' field is present (if missing tries dt + 'T00:00:00Z')
  - Skips existing JSONL unless --overwrite is supplied
"""
from __future__ import annotations
import argparse, pathlib, csv, json, sys


def rebuild(raw_dir: pathlib.Path, out_dir: pathlib.Path, overwrite: bool = False) -> dict:
    if not raw_dir.exists():
        raise SystemExit(f"raw dir not found: {raw_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = { 'processed_partitions': 0, 'written_files': 0, 'skipped': 0 }
    for dt_dir in sorted(raw_dir.glob('dt=*')):
        if not dt_dir.is_dir():
            continue
        try:
            dt_val = dt_dir.name.split('dt=')[1]
        except Exception:
            continue
        csv_files = list(dt_dir.glob('*.csv'))
        if not csv_files:
            continue
        # Combine all CSV rows for day (usually one file)
        rows = []
        for csv_file in csv_files:
            with csv_file.open() as f:
                r = csv.DictReader(f)
                for row in r:
                    if not row.get('timestamp') and dt_val:
                        row['timestamp'] = f"{dt_val}T00:00:00Z"
                    rows.append(row)
        out_file = out_dir / f"all-visits-{dt_val}.jsonl"
        if out_file.exists() and not overwrite:
            summary['skipped'] += 1
            continue
        with out_file.open('w') as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        summary['written_files'] += 1
        summary['processed_partitions'] += 1
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw-dir', default='data/raw/page_count', help='Path to raw page_count directory')
    ap.add_argument('--out-dir', default='_tmp_downloads', help='Output directory for JSONL files')
    ap.add_argument('--overwrite', action='store_true', help='Overwrite existing JSONL')
    args = ap.parse_args()
    raw_dir = pathlib.Path(args.raw_dir)
    out_dir = pathlib.Path(args.out_dir)
    summary = rebuild(raw_dir, out_dir, overwrite=args.overwrite)
    print(json.dumps(summary, indent=2))

if __name__ == '__main__':
    main()

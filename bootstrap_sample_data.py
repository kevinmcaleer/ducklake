#!/usr/bin/env python
"""Create small sample raw data for dev/testing and run a refresh.

Usage:
  python bootstrap_sample_data.py --db contentlake.ducklake
"""
from __future__ import annotations
import argparse, pathlib, datetime
from ducklake_core.simple_pipeline import RAW_DIR
from run_refresh import main as refresh_main

PAGE_COUNT_ROWS = [
  {"timestamp": "2025-09-18T10:01:00Z", "ip": "1.1.1.1", "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/118.0"},
  {"timestamp": "2025-09-18T10:02:00Z", "ip": "1.1.1.2", "user_agent": "Googlebot/2.1 (+http://www.google.com/bot.html)"},
]
SEARCH_LOG_ROWS = [
  {"timestamp": "2025-09-18T10:05:00Z", "ip": "1.1.1.1", "query": "Ducklake"},
  {"timestamp": "2025-09-18T10:06:00Z", "ip": "1.1.1.2", "query": "data pipeline"},
]


def _write_csv(rows, out_file: pathlib.Path):
    if not rows:
        return
    cols = sorted({k for r in rows for k in r.keys()})
    import csv
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def build_sample_data(today: datetime.date):
    # Adjust timestamps to today
    pc_rows = []
    for r in PAGE_COUNT_ROWS:
        dt_part = today.isoformat()
        ts = r['timestamp']
        r_copy = dict(r)
        r_copy['timestamp'] = dt_part + ts[10:]  # replace date portion
        pc_rows.append(r_copy)
    sl_rows = []
    for r in SEARCH_LOG_ROWS:
        dt_part = today.isoformat()
        ts = r['timestamp']
        r_copy = dict(r)
        r_copy['timestamp'] = dt_part + ts[10:]
        sl_rows.append(r_copy)

    pc_dir = RAW_DIR / 'page_count' / f'dt={today.isoformat()}'
    sl_dir = RAW_DIR / 'search_logs' / f'dt={today.isoformat()}'
    _write_csv(pc_rows, pc_dir / 'page_count.csv')
    _write_csv(sl_rows, sl_dir / 'search_logs.csv')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='contentlake.ducklake')
    args = ap.parse_args()
    today = datetime.date.today()
    build_sample_data(today)
    refresh_main(['--db', args.db, '--json'])

if __name__ == '__main__':
    main()

import csv
import os
from pathlib import Path
from collections import defaultdict

SRC = Path('_tmp_downloads/all-visits-full.csv')
RAW_DIR = Path('data/raw/page_count')

RAW_DIR.mkdir(parents=True, exist_ok=True)

def split_all_visits_full(src, raw_dir):
    files = defaultdict(list)
    with open(src, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            # Try to find a date column (timestamp, dt, or date)
            dt = None
            for key in ('timestamp', 'dt', 'date'):
                if key in row and row[key]:
                    dt = row[key][:10]
                    break
            if not dt:
                continue
            files[dt].append(row)
    for dt, rows in files.items():
        target_dir = raw_dir / f'dt={dt}'
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = target_dir / 'page_count.csv'
        with open(out_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f'Wrote {out_path} ({len(rows)} rows)')

if __name__ == '__main__':
    split_all_visits_full(SRC, RAW_DIR)

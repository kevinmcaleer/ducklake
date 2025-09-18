import csv
import os
from collections import defaultdict
from datetime import datetime

SRC = '_tmp_downloads/search_logs_csv/search-logs.csv'
DST_DIR = '_tmp_downloads/search_logs_daily'

os.makedirs(DST_DIR, exist_ok=True)

def split_search_logs_by_date(src, dst_dir):
    files = defaultdict(list)
    with open(src, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            ts = row.get('timestamp')
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts).date()
            except Exception:
                continue
            files[str(dt)].append(row)
    for dt, rows in files.items():
        target_dir = os.path.join('data', 'raw', 'search_logs', f'dt={dt}')
        os.makedirs(target_dir, exist_ok=True)
        out_path = os.path.join(target_dir, 'search_logs.csv')
        with open(out_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f'Wrote {out_path} ({len(rows)} rows)')

if __name__ == '__main__':
    split_search_logs_by_date(SRC, DST_DIR)

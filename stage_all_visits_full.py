import csv
import os
from collections import defaultdict
from datetime import datetime

SRC = '_tmp_downloads/all-visits-full.csv'
DST_DIR = '_tmp_downloads'

# Read and split by date
def split_csv_by_date(src, dst_dir):
    files = defaultdict(list)
    with open(src, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            # Try both 'timestamp' and 'ts' as possible time columns
            ts = row.get('timestamp') or row.get('ts')
            if not ts:
                continue
            # Normalize date format
            try:
                dt = datetime.fromisoformat(ts.replace(' ', 'T')).date()
            except Exception:
                continue
            files[str(dt)].append(row)
    # Write out one file per date
    for dt, rows in files.items():
        out_path = os.path.join(dst_dir, f'all-visits-{dt}.csv')
        with open(out_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f'Wrote {out_path} ({len(rows)} rows)')

if __name__ == '__main__':
    split_csv_by_date(SRC, DST_DIR)

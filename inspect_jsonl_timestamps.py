import json
import pathlib
from collections import Counter
from datetime import datetime

# Path to the file to analyze
FILE = pathlib.Path("_tmp_downloads/all-visits-2025-09-07.jsonl")

timestamps = []
with FILE.open("r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            ts = rec.get("timestamp")
            if ts:
                timestamps.append(ts)
        except Exception as e:
            print(f"Error parsing line: {e}")

if not timestamps:
    print("No timestamps found.")
else:
    # Print min, max, and a sample
    dt_objs = []
    for ts in timestamps:
        try:
            dt_objs.append(datetime.fromisoformat(ts.replace("Z", "")))
        except Exception:
            pass
    if dt_objs:
        print(f"Earliest timestamp: {min(dt_objs)}")
        print(f"Latest timestamp: {max(dt_objs)}")
    print(f"Total records with timestamp: {len(timestamps)}")
    print("Sample timestamps:", timestamps[:5])
    # Count by date
    date_counts = Counter(ts[:10] for ts in timestamps if len(ts) >= 10)
    print("Counts by date:")
    for d, c in date_counts.most_common():
        print(f"  {d}: {c}")

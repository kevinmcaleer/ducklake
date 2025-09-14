
import json
import sys
from collections import Counter

def main():
    if len(sys.argv) < 2:
        print("Usage: python parse_jsonl.py <file>")
        sys.exit(1)
    filename = sys.argv[1]
    timestamps = []
    records = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                print(f"Error parsing line: {e}")
                continue
            # Skip summary lines (only 'total_count' key)
            if isinstance(obj, dict) and set(obj.keys()) == {"total_count"}:
                continue
            records.append(obj)
            if "timestamp" in obj:
                timestamps.append(obj["timestamp"])
    if not records:
        print("No records found in file.")
        return
    print(f"First record: {records[0]}")
    print(f"All keys: {list(records[0].keys())}")
    if timestamps:
        # Count by date
        dates = [ts[:10] for ts in timestamps]
        counter = Counter(dates)
        print("Counts by date:")
        for date, count in sorted(counter.items()):
            print(f"  {date}: {count}")
    else:
        print("No timestamps found in file.")
    print(f"Total records (excluding summary): {len(records)}")

if __name__ == "__main__":
    main()
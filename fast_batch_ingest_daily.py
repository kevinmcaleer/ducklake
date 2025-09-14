import pathlib
import re
from intake import ingest_bronze

# Directory containing daily files

DAILY_DIR = pathlib.Path("_tmp_downloads")
PATTERN = re.compile(r"all-visits-(\d{4}-\d{2}-\d{2})\.jsonl$")

files = sorted(DAILY_DIR.glob("all-visits-*.jsonl"))
total = len(files)
print(f"Found {total} files to ingest.")

for idx, file in enumerate(files, 1):
    m = PATTERN.match(file.name)
    if not m:
        print(f"[{idx}/{total}] Skipping {file.name}: does not match pattern")
        continue
    dt = m.group(1)
    print(f"[{idx}/{total}] Ingesting {file.name} for dt={dt} ...", end=" ")
    try:
        dest, status = ingest_bronze("page_count", str(file), "json", dt_override=dt)
        if status == "ok":
            print("✔️")
        else:
            print(f"❌ (status={status})")
    except Exception as e:
        print(f"❌ Exception: {e}")
print("Ingestion complete.")

# Directory containing daily files

DAILY_DIR = pathlib.Path("_tmp_downloads")
PATTERN = re.compile(r"all-visits-(\d{4}-\d{2}-\d{2})\.csv$")

files = sorted(DAILY_DIR.glob("all-visits-*.csv"))
total = len(files)
print(f"Found {total} files to ingest.")

for idx, file in enumerate(files, 1):
    m = PATTERN.match(file.name)
    if not m:
        print(f"[{idx}/{total}] Skipping {file.name}: does not match pattern")
        continue
    dt = m.group(1)
    print(f"[{idx}/{total}] Ingesting {file.name} for dt={dt} ...", end=" ")
    try:
        dest, status = ingest_bronze("page_count", str(file), "csv", dt_override=dt)
        if status == "ok":
            print("✔️")
        else:
            print(f"❌ (status={status})")
    except Exception as e:
        print(f"❌ Exception: {e}")
print("Ingestion complete.")

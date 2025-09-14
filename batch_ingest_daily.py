import os
import pathlib
import subprocess
import re
from datetime import datetime

# Directory containing daily files (update this path as needed)
DAILY_DIR = pathlib.Path("_tmp_downloads")
# File pattern: all-visits-YYYY-MM-DD.jsonl
PATTERN = re.compile(r"all-visits-(\d{4}-\d{2}-\d{2})\.jsonl$")

for file in DAILY_DIR.glob("all-visits-*.csv"):
    m = PATTERN.match(file.name)
    if not m:
        print(f"Skipping {file.name}: does not match pattern")
        continue
    dt = m.group(1)
    cmd = [
        "python", "intake.py", "ingest-file", "page_count",
    str(file), "--format", "csv", "--dt", dt
    ]
    print(f"Ingesting {file.name} for dt={dt} ...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✔️ Ingested {file.name}")
    else:
        print(f"❌ Failed to ingest {file.name}: {result.stderr}")

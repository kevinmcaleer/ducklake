import os
import shutil
import re
from pathlib import Path

RAW_DIR = Path("data/raw/search_logs")
pattern = re.compile(r"search-logs-(\d{4}-\d{2}-\d{2})\\.csv")

for file in RAW_DIR.glob("search-logs-*.csv"):
    m = pattern.match(file.name)
    if not m:
        continue
    dt = m.group(1)
    target_dir = RAW_DIR / f"dt={dt}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "search_logs.csv"
    print(f"Moving {file} -> {target_file}")
    shutil.move(str(file), str(target_file))

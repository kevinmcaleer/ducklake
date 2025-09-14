
import requests
import pathlib
import json
from datetime import datetime, timedelta
import time

# Config
BASE_URL = "http://page_count.kevsrobots.com/all-visits"
DOWNLOAD_DIR = pathlib.Path("_tmp_downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Set the earliest date to stop (or None to keep going until no data)
EARLIEST_DATE = None  # e.g., '2024-01-01'

# Start from today
current = datetime.utcnow().date()

empty_days_in_month = 0
current_month = current.month

while True:
    next_day = current + timedelta(days=1)
    range_param = f"range={current},{next_day}"
    url = f"{BASE_URL}?{range_param}&format=jsonl"
    out_path = DOWNLOAD_DIR / f"all-visits-{current}.jsonl"
    print(f"Downloading {url} ...")
    resp = requests.get(url, timeout=60)
    print(f"\nStatus code: {resp.status_code}")
    print(f"Headers: {resp.headers}\n")
    # Print first 500 characters of body for debug
    print(f"Body (first 500 chars): {resp.text[:500]!r}\n")
    if resp.status_code != 200:
        print(f"❌ HTTP {resp.status_code} for {url}")
        break
    # Write raw response text as JSONL
    with out_path.open("w", encoding="utf-8") as f:
        f.write(resp.text)
    print(f"✔️ Saved {out_path}")
    # If file is empty, continue to next date and count empty days
    if out_path.stat().st_size == 0:
        print(f"No data for {current}, continuing to next date.")
        out_path.unlink()
        if current.month == current_month:
            empty_days_in_month += 1
        else:
            # New month, reset counter
            current_month = current.month
            empty_days_in_month = 1
        # If we've seen 28+ empty days in a month, assume month is empty and stop
        if empty_days_in_month >= 28:
            print(f"No data for entire month {current.strftime('%Y-%m')}, stopping.")
            break
    else:
        # Reset empty day counter if we get data
        if current.month != current_month:
            current_month = current.month
        empty_days_in_month = 0
    # Go to previous day
    current -= timedelta(days=1)
    time.sleep(0.5)  # Be nice to the server
    # Stop if reached earliest date
    if EARLIEST_DATE and str(current) <= EARLIEST_DATE:
        break
    # Go to previous day
    current -= timedelta(days=1)
    time.sleep(0.01)  # Be nice to the server

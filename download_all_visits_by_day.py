
import requests
import pathlib
from datetime import datetime, timedelta, timezone
import time
import sys
import os
import argparse
import subprocess

# This script walks backwards day-by-day from today, downloading visit data.
# Improvements:
#  - Only writes a file if the response has data (non-empty body)
#  - Stops after N (default 10) consecutive empty days (previously per-month 28 heuristic)
#  - Fixes prior double date decrement bug
#  - Provides simple progress output and final summary

MAX_CONSECUTIVE_EMPTY_DAYS = int((os.environ.get("MAX_CONSECUTIVE_EMPTY_DAYS") or 20))

parser = argparse.ArgumentParser(description="Download daily page_count snapshots backwards from today")
parser.add_argument('--format', choices=['jsonl','csv'], default='csv', help='Download format (default: csv)')
parser.add_argument('--max-empty', type=int, default=MAX_CONSECUTIVE_EMPTY_DAYS, help='Consecutive empty day stop threshold')
parser.add_argument('--start', type=str, default=None, help='Start date (YYYY-MM-DD) instead of today')
parser.add_argument('--days', type=int, default=None, help='Maximum number of days to fetch (cap)')
parser.add_argument('--earliest', type=str, default=None, help='Earliest date bound (YYYY-MM-DD)')
parser.add_argument('--sleep', type=float, default=0.25, help='Base sleep between requests when data found')
parser.add_argument('--sleep-empty', type=float, default=0.05, help='Sleep between requests when empty day')
parser.add_argument('--ingest', action='store_true', help='After download, ingest file into bronze via intake.py ingest-file page_count')
parser.add_argument('--ingest-format', default=None, help='Override ingest format (defaults to same as download)')
parser.add_argument('--force', action='store_true', help='Force re-download even if local file already exists')
parser.add_argument('--no-skip-existing', action='store_true', help='Do not skip existing files (deprecated; use --force)')
parser.add_argument('--refresh-open-days', type=int, default=2, help='Always re-fetch the most recent N days (default: 2)')
parser.add_argument('--hash-check', action='store_true', help='Use SHA256 sidecar to detect content changes for open days')
args = parser.parse_args()

BASE_URL = "http://page_count.kevsrobots.com/all-visits"  # base endpoint
DOWNLOAD_DIR = pathlib.Path("_tmp_downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

EARLIEST_DATE = args.earliest

if args.start:
    current = datetime.strptime(args.start, '%Y-%m-%d').date()
else:
    current = datetime.now(timezone.utc).date()
consecutive_empty = 0
downloaded = 0
updated = 0
skipped_empty = 0
skipped_existing = 0
unchanged = 0
start_date = current

def compute_sha256(text: str) -> str:
    import hashlib
    h = hashlib.sha256()
    h.update(text.encode('utf-8'))
    return h.hexdigest()

def log(msg: str):
    ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")

log(f"Starting backward download from {current} (stop after {args.max_empty} empty days, format={args.format})")
if EARLIEST_DATE:
    log(f"Earliest bound: {EARLIEST_DATE}")
if args.days:
    log(f"Max days to fetch: {args.days}")

open_window_cutoff = datetime.now(timezone.utc).date() - timedelta(days=parser.parse_args().refresh_open_days - 1)

while True:
    next_day = current + timedelta(days=1)
    ext = 'jsonl' if args.format == 'jsonl' else 'csv'
    out_path = DOWNLOAD_DIR / f"all-visits-{current}.{ext}"
    hash_path = out_path.with_suffix(out_path.suffix + '.sha256')
    is_open_day = current >= open_window_cutoff
    need_download = True
    if out_path.exists() and out_path.stat().st_size > 0 and not args.force:
        if not is_open_day and not args.no_skip_existing:
            skipped_existing += 1
            need_download = False
        elif is_open_day:
            # Will re-fetch to see if content grew
            pass
    if need_download:
        url = f"{BASE_URL}?range={current},{next_day}&format={args.format}"
        log(f"GET {url} (open={is_open_day} exists={out_path.exists()} size={out_path.stat().st_size if out_path.exists() else 0})")
        try:
            resp = requests.get(url, timeout=30)
        except Exception as e:
            log(f"Request error on {current}: {e}; stopping.")
            break
        if resp.status_code != 200:
            log(f"HTTP {resp.status_code} (expected 200); stopping.")
            break
        body = resp.text
        if not body.strip():
            consecutive_empty += 1
            skipped_empty += 1
            log(f"No data for {current} (consecutive empty: {consecutive_empty})")
            if consecutive_empty >= args.max_empty:
                log("Reached max consecutive empty days; stopping.")
                break
        else:
            new_hash = compute_sha256(body) if args.hash_check else None
            old_hash = hash_path.read_text().strip() if (args.hash_check and hash_path.exists()) else None
            content_changed = (not args.hash_check) or (new_hash != old_hash)
            if not content_changed and is_open_day and out_path.exists():
                unchanged += 1
                log(f"Unchanged (hash) {out_path}; skipping write")
            else:
                try:
                    out_path.write_text(body, encoding="utf-8")
                    if new_hash:
                        hash_path.write_text(new_hash, encoding='utf-8')
                    if out_path.exists() and old_hash and new_hash and new_hash != old_hash:
                        updated += 1
                        log(f"Updated {out_path} (hash changed)")
                    elif not old_hash and not out_path.exists():
                        downloaded += 1
                    elif not old_hash:
                        downloaded += 1
                    else:
                        # changed content without hash_check path
                        if content_changed:
                            updated += 1
                    log(f"Saved {out_path} ({len(body)} bytes)")
                    if args.ingest:
                        ingest_fmt = args.ingest_format or ext
                        cmd = [sys.executable, 'intake.py', 'ingest-file', 'page_count', str(out_path), '--format', ingest_fmt, '--dt', str(current)]
                        log(f"Ingesting via: {' '.join(cmd)}")
                        try:
                            r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                            if r.returncode != 0:
                                log(f"Ingest failed rc={r.returncode}: {r.stderr.strip()[:300]}")
                            else:
                                log(f"Ingest ok: {r.stdout.strip()[:200]}")
                        except Exception as e:
                            log(f"Ingest exception: {e}")
                except Exception as e:
                    log(f"Failed writing file for {current}: {e}")
            consecutive_empty = 0
    else:
        # treat as data present (closed day already stored)
        consecutive_empty = 0
    # Move one day back
    current -= timedelta(days=1)
    if EARLIEST_DATE and str(current) <= EARLIEST_DATE:
        log("Reached earliest date bound; stopping.")
        break
    if args.days and (start_date - current).days >= args.days:
        log("Reached max days limit; stopping.")
        break
    # Gentle pacing
    time.sleep(args.sleep if consecutive_empty == 0 else args.sleep_empty)

log("Done.")
log(f"Summary: downloaded={downloaded} updated={updated} unchanged={unchanged} skipped_existing={skipped_existing} empty_empty_days={skipped_empty} start_date={start_date} end_date={current}")
if downloaded == 0:
    sys.exit(1)

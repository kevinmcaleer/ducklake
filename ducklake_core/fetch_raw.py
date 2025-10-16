import os, json, pathlib, datetime, requests, csv, re, sys
from urllib.parse import urlparse, parse_qs, unquote
from .simple_pipeline import RAW_DIR

DEFAULT_TMP_DOWNLOADS = pathlib.Path('_tmp_downloads')

PAGE_COUNT_ENV_URL = 'PAGE_COUNT_API_URL'  # Expected base URL e.g. https://example/api/page_count
PAGE_COUNT_ENV_KEY = 'PAGE_COUNT_API_KEY'  # Optional auth token

# Hard-coded default page_count collection endpoint (user confirmed it is stable)
DEFAULT_PAGE_COUNT_BASE_URL = 'http://page_count.kevsrobots.com/all-visits'

SEARCH_LOGS_ENV_URL = 'SEARCH_LOGS_API_URL'
SEARCH_LOGS_ENV_KEY = 'SEARCH_LOGS_API_KEY'


def _progress(msg: str, quiet: bool = False):
    """Print progress message to stderr unless quiet mode."""
    if not quiet:
        print(f"[progress] {msg}", file=sys.stderr, flush=True)


def _iso_date(ts: str):
    try:
        return datetime.datetime.fromisoformat(ts.replace('Z','+00:00')).date()
    except Exception:
        try:
            return datetime.date.fromisoformat(ts[:10])
        except Exception:
            return None


def _write_rows_per_day(rows, out_base: pathlib.Path, filename: str, overwrite: bool, quiet: bool = False):
    _progress(f"Grouping {len(rows)} rows by date...", quiet)
    by_day = {}
    for r in rows:
        ts = r.get('timestamp') or r.get('event_ts') or r.get('time')
        dt = _iso_date(ts) if ts else None
        if not dt:
            continue
        by_day.setdefault(dt, []).append(r)
    _progress(f"Writing {len(by_day)} daily partitions...", quiet)
    out_summary = {}
    for dt, day_rows in by_day.items():
        dt_dir = out_base / f'dt={dt}'
        dt_dir.mkdir(parents=True, exist_ok=True)
        out_file = dt_dir / filename
        if out_file.exists() and not overwrite:
            out_summary[str(dt)] = {'rows': len(day_rows), 'skipped': True}
            continue
        # Infer columns from union of keys to keep flexible
        cols = sorted({k for r in day_rows for k in r.keys()})
        with out_file.open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in day_rows:
                w.writerow({k: r.get(k) for k in cols})
        out_summary[str(dt)] = {'rows': len(day_rows), 'skipped': False}
    return out_summary


def fetch_page_count_recent(days: int, overwrite: bool = False, base_url: str | None = None, api_key: str | None = None, fallback_dir: pathlib.Path | None = None):
    """Fetch last N days of page_count events and write raw CSV partitions with diagnostics.

    Added diagnostics fields to help troubleshoot missing / stale data situations:
      - api: { attempted: bool, url, status_code, error, rows_before_flatten, rows_after_flatten }
      - fallback: { used: bool, dir, files_considered, files_loaded }
      - observed_dates: sorted list of distinct dt parsed from timestamp/event_ts/time
      - earliest_ts / latest_ts (raw ISO strings if present)
      - expected_recent_dates: list of date strings we attempted to fetch (today - i)
      - missing_recent_dates: expected - observed intersection for quick gap view
      - freshness_gap_days: (today - max(observed_date)) if any rows else None
      - zero_rows_reason when 0 rows (api_empty | api_error | no_fallback_files)
    """
    _progress(f"Fetching page_count for last {days} day(s)...")
    base_url = base_url or os.getenv(PAGE_COUNT_ENV_URL) or DEFAULT_PAGE_COUNT_BASE_URL
    api_key = api_key or os.getenv(PAGE_COUNT_ENV_KEY)
    out_base = RAW_DIR / 'page_count'
    out_base.mkdir(parents=True, exist_ok=True)

    today = datetime.date.today()
    expected_recent_dates = [str(today - datetime.timedelta(days=i)) for i in range(days-1, -1, -1)]

    rows: list[dict] = []
    api_diag = {
        'attempted': False,
        'attempted_urls': [],
        'status_code': None,
        'error': None,
        'rows_before_flatten': 0,
        'rows_after_flatten': 0,
    }
    if base_url:
        headers = {'Authorization': f"Bearer {api_key}"} if api_key else {}
        # Attempt strategy hierarchy:
        # 1) range={days}d with since = earliest expected date (oldest) -> expects list or JSONL
        # 2) fallback to recent=days (legacy)
        earliest_expected = expected_recent_dates[0]
        candidate_urls = [
            f"{base_url}?range={days}d&since={earliest_expected}",
            (f"{base_url}?recent={days}" if 'recent=' not in base_url else base_url),
        ]
        for attempt_url in candidate_urls:
            if rows:  # stop after first success with rows
                break
            api_diag['attempted'] = True
            api_diag['attempted_urls'].append(attempt_url)
            _progress(f"Requesting page_count from API...")
            try:
                resp = requests.get(attempt_url, headers=headers, timeout=60)
                api_diag['status_code'] = resp.status_code
                resp.raise_for_status()
                txt = resp.text.strip()
                if txt.startswith('['):
                    data = resp.json()
                    if isinstance(data, list):
                        rows.extend(data)
                else:
                    for line in txt.splitlines():
                        line=line.strip()
                        if not line:
                            continue
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            continue
            except Exception as e:
                # Record last error but continue to next attempt
                api_diag['error'] = repr(e)

    api_diag['rows_before_flatten'] = len(rows)
    # Flatten wrapper objects like {"visits": [...]} if present
    if rows:
        _progress(f"Processing {len(rows)} page_count records...")
        flattened: list[dict] = []
        for r in rows:
            if isinstance(r, dict) and 'visits' in r and isinstance(r['visits'], list):
                for v in r['visits']:
                    if isinstance(v, dict):
                        flattened.append(v)
            else:
                flattened.append(r)
        rows = flattened
    api_diag['rows_after_flatten'] = len(rows)

    fb_diag = {
        'used': False,
        'dir': None,
        'files_considered': 0,
        'files_loaded': 0,
    }
    if not rows:
        _progress("No API data, checking fallback files...")
        fb = fallback_dir or DEFAULT_TMP_DOWNLOADS
        fb_diag['dir'] = str(fb)
        if fb.exists():
            files = sorted(fb.glob('all-visits-*.jsonl'))[-days:]
            fb_diag['files_considered'] = len(files)
            for p in files:
                try:
                    loaded_any = False
                    for line in p.read_text().splitlines():
                        line=line.strip()
                        if not line: continue
                        try:
                            rows.append(json.loads(line)); loaded_any = True
                        except Exception:
                            continue
                    if loaded_any:
                        fb_diag['files_loaded'] += 1
                except Exception:
                    continue
        fb_diag['used'] = len(rows) > 0

    # Derive observed date set & ts range
    observed_dates_set = set()
    earliest_ts = None
    latest_ts = None
    for r in rows:
        ts = r.get('timestamp') or r.get('event_ts') or r.get('time')
        if ts:
            # Normalize parse to date for list
            dt_obj = _iso_date(str(ts))
            if dt_obj:
                observed_dates_set.add(dt_obj)
            # maintain min/max lexicographically by ISO (after coercion)
            try:
                # Force ISO-like comparable string
                iso = str(ts)
                if earliest_ts is None or iso < earliest_ts:
                    earliest_ts = iso
                if latest_ts is None or iso > latest_ts:
                    latest_ts = iso
            except Exception:
                pass
    observed_dates = sorted(str(d) for d in observed_dates_set)
    max_dt = max(observed_dates_set) if observed_dates_set else None
    freshness_gap_days = (today - max_dt).days if max_dt else None
    missing_recent_dates = [d for d in expected_recent_dates if d not in observed_dates]

    zero_rows_reason = None
    if len(rows) == 0:
        if api_diag['attempted'] and api_diag['error']:
            zero_rows_reason = 'api_error'
        elif api_diag['attempted'] and api_diag['status_code'] and api_diag['status_code'] == 200:
            zero_rows_reason = 'api_empty'
        else:
            zero_rows_reason = 'no_fallback_files'

    written = _write_rows_per_day(rows, out_base, 'page_count.csv', overwrite)
    return {
        'source': 'page_count',
        'days': days,
        'written': written,
        'api': api_diag,
        'fallback': fb_diag,
        'observed_dates': observed_dates,
        'expected_recent_dates': expected_recent_dates,
        'missing_recent_dates': missing_recent_dates,
        'earliest_ts': earliest_ts,
        'latest_ts': latest_ts,
        'freshness_gap_days': freshness_gap_days,
        'zero_rows_reason': zero_rows_reason,
    }


def fetch_search_logs_snapshot(snapshot_url: str | None = None, overwrite: bool = False, base_url: str | None = None, api_key: str | None = None):
    """Fetch search logs either from provided snapshot_url (JSONL) or SEARCH_LOGS_API_URL env.

    Writes per-day CSV partitions using timestamp/event_ts/time detection.
    """
    _progress("Fetching search_logs snapshot...")
    base_url = snapshot_url or base_url or os.getenv(SEARCH_LOGS_ENV_URL)
    api_key = api_key or os.getenv(SEARCH_LOGS_ENV_KEY)
    out_base = RAW_DIR / 'search_logs'
    out_base.mkdir(parents=True, exist_ok=True)
    rows = []
    if base_url:
        _progress(f"Requesting search_logs from API...")
        headers = {'Authorization': f"Bearer {api_key}"} if api_key else {}
        try:
            resp = requests.get(base_url, headers=headers, timeout=120)
            resp.raise_for_status()
            _progress(f"Processing search_logs response...")
            txt = resp.text.strip()
            if txt.startswith('['):
                data = resp.json()
                rows.extend(data if isinstance(data, list) else [])
            else:
                for line in txt.splitlines():
                    line=line.strip()
                    if not line: continue
                    try: rows.append(json.loads(line))
                    except Exception: continue
        except Exception:
            pass
    return {'source': 'search_logs', 'written': _write_rows_per_day(rows, out_base, 'search_logs.csv', overwrite)}


def fetch_search_logs_file(path: str, overwrite: bool = False, assume_utc: bool = True):
    """Parse a local search logs file (line- or JSON-delimited) into per-day raw partitions.

    Parsing strategy (in order):
      1. JSON object per line -> extract timestamp fields (timestamp/event_ts/time/ts/created_at) and query fields (query/q/term/search/keyword).
         If missing query but URL-like field present (url/path/request), parse query string (?q= or ?query=) and decode.
      2. CSV-style line: first field matches date or date-time -> attempt to locate query tokens in remaining fields.
      3. Plain text line: regex for timestamp (ISO with 'T' OR space) then locate URL or key=value tokens (query=, q=, search=).

    Additional heuristics:
      - Support timestamps like 'YYYY-MM-DD HH:MM:SS' by converting space to 'T'.
      - Percent-decoding of query parameter values.
      - Filters out lines missing either timestamp or query after attempts.

    Returns detailed counters for diagnostics.
    """
    _progress(f"Parsing search logs from file: {path}")
    file_path = pathlib.Path(path)
    out_base = RAW_DIR / 'search_logs'
    out_base.mkdir(parents=True, exist_ok=True)
    if not file_path.exists():
        return {'source': 'search_logs', 'error': f'file not found: {file_path}'}

    # Broader timestamp regex: ISO with T or space, optional timezone offset or Z
    ts_regex = re.compile(r'(20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:?\d{2})?)')
    # Quick date-only (fallback) if needed
    date_only_regex = re.compile(r'^(20\d{2}-\d{2}-\d{2})$')

    def norm_ts(raw: str | None):
        if not raw:
            return None
        raw = raw.strip()
        # If space-separated date time convert to T
        if re.match(r'^20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', raw):
            return raw.replace(' ', 'T') + ('Z' if assume_utc else '')
        if date_only_regex.match(raw):
            # date only -> append midday for determinism
            return raw + 'T12:00:00' + ('Z' if assume_utc else '')
        return raw

    query_keys = ('query','q','term','search','keyword')
    url_keys = ('url','path','request')
    ip_keys = ('ip','remote_addr','client_ip')

    rows = []
    total = 0
    parsed = 0
    skipped_no_ts = 0
    skipped_no_query = 0

    _progress(f"Reading search logs file...")
    lines = file_path.read_text(errors='ignore').splitlines()
    _progress(f"Parsing {len(lines)} lines from search logs...")
    for raw_line in lines:
        total += 1
        if total % 1000 == 0:
            _progress(f"Parsed {parsed} records from {total} lines...")
        line = raw_line.strip()
        if not line:
            continue
        record = None
        ts_val = None  # ensure defined for skip accounting
        # Pattern: INFO:root:2024-01-07T13:13:28.736047 - IP: 192.168.1.4 - Query: python
        if record is None and line.startswith('INFO:root:') and ' - IP:' in line and ' - Query:' in line:
            try:
                rest = line.split('INFO:root:',1)[1]
                ts_part, remainder = rest.split(' - IP:',1)
                ts_val = norm_ts(ts_part.strip())
                ip_part, query_part = remainder.split(' - Query:',1)
                ip_val = ip_part.strip()
                q_val = query_part.strip()
                if ts_val and q_val:
                    record = {'timestamp': ts_val, 'query': q_val, 'ip': ip_val}
            except Exception:
                record = None
        # 1. JSON attempt
        if record is None and line.startswith('{') and line.endswith('}'):
            try:
                obj = json.loads(line)
                ts_val = None
                for k in ('timestamp','event_ts','time','ts','created_at'):
                    if k in obj and obj[k]:
                        ts_val = norm_ts(str(obj[k]))
                        break
                q_val = None
                for k in query_keys:
                    if k in obj and obj[k]:
                        q_val = str(obj[k])
                        break
                if not q_val:
                    for k in url_keys:
                        if k in obj and obj[k]:
                            try:
                                u = obj[k]
                                if isinstance(u, str):
                                    u_str = u if '://' in u else 'http://local' + u
                                    qp = parse_qs(urlparse(u_str).query)
                                    for cand in ('q','query','term','search','keyword'):
                                        if cand in qp:
                                            q_val = unquote(qp[cand][0])
                                            break
                                    if q_val:
                                        break
                            except Exception:
                                pass
                ip_val = None
                for k in ip_keys:
                    if k in obj and obj[k]:
                        ip_val = str(obj[k])
                        break
                if ts_val and q_val:
                    record = {'timestamp': ts_val, 'query': q_val, 'ip': ip_val}
            except Exception:
                record = None
        # 2. CSV-like (fallback) if not JSON and contains commas
        if record is None and ',' in line:
            parts = [p.strip() for p in line.split(',')]
            if parts:
                first = parts[0]
                if ts_regex.match(first) or re.match(r'^20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', first) or date_only_regex.match(first):
                    ts_val = norm_ts(first)
                    q_val = None
                    # search remaining fields for query=..., q=..., or treat second column as query heuristic
                    for p in parts[1:]:
                        if 'query=' in p:
                            q_val = p.split('query=',1)[1]
                            break
                        if 'q=' in p:
                            q_val = p.split('q=',1)[1]
                            break
                    if not q_val and len(parts) > 1:
                        q_val = parts[1]
                    if ts_val and q_val:
                        record = {'timestamp': ts_val, 'query': q_val}
        # 3. Plain text heuristics
        if record is None:
            ts_match_obj = ts_regex.search(line)
            ts_val = norm_ts(ts_match_obj.group(1)) if ts_match_obj else None
            q_val = None
            # Extract URL and parse query params
            url_match = re.search(r'(https?://\S+)', line)
            if url_match:
                try:
                    u = url_match.group(1)
                    qp = parse_qs(urlparse(u).query)
                    for cand in ('q','query','term','search','keyword'):
                        if cand in qp:
                            q_val = unquote(qp[cand][0])
                            break
                except Exception:
                    pass
            if not q_val:
                # key=value tokens
                for token in line.split():
                    if token.startswith('query=') or token.startswith('q=') or token.startswith('search='):
                        q_val = token.split('=',1)[1].strip('"')
                        break
            if ts_val and q_val:
                record = {'timestamp': ts_val, 'query': q_val}
        if record:
            parsed += 1
            rows.append(record)
        else:
            if ts_val:
                skipped_no_query += 1
            else:
                skipped_no_ts += 1

    written = _write_rows_per_day(rows, out_base, 'search_logs.csv', overwrite)
    return {
        'source': 'search_logs',
        'mode': 'file',
        'rows': len(rows),
        'days': len(written),
        'written': written,
        'total_lines': total,
        'parsed_lines': parsed,
        'skipped_no_timestamp': skipped_no_ts,
        'skipped_no_query': skipped_no_query
    }


def fetch_page_count_all(overwrite: bool = False, base_url: str | None = None, api_key: str | None = None, fallback_dir: pathlib.Path | None = None):
    """Fetch ALL available page_count events and write raw CSV partitions with diagnostics.

    Diagnostics similar to fetch_page_count_recent (without expected_recent_dates) to aid full backfill troubleshooting.
    """
    _progress("Fetching ALL page_count history...")
    base_url = base_url or os.getenv(PAGE_COUNT_ENV_URL) or DEFAULT_PAGE_COUNT_BASE_URL
    api_key = api_key or os.getenv(PAGE_COUNT_ENV_KEY)
    out_base = RAW_DIR / 'page_count'
    out_base.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []

    api_diag = {
        'attempted': False,
        'attempted_urls': [],
        'status_code': None,
        'error': None,
        'rows_before_flatten': 0,
        'rows_after_flatten': 0,
    }
    if base_url:
        # Strategy attempts: range=all, then all=1 legacy param
        candidate_urls = []
        candidate_urls.append(f"{base_url}?range=all")
        if 'all=' not in base_url:
            if '?' in base_url:
                candidate_urls.append(base_url + '&all=1')
            else:
                candidate_urls.append(base_url + '?all=1')
        else:
            candidate_urls.append(base_url)
        headers = {'Authorization': f"Bearer {api_key}"} if api_key else {}
        for attempt_url in candidate_urls:
            if rows:
                break
            api_diag['attempted'] = True
            api_diag['attempted_urls'].append(attempt_url)
            _progress(f"Requesting full page_count history from API (may take a while)...")
            try:
                resp = requests.get(attempt_url, headers=headers, timeout=180)
                api_diag['status_code'] = resp.status_code
                resp.raise_for_status()
                txt = resp.text.strip()
                try:
                    data = resp.json()
                    if isinstance(data, list):
                        rows.extend(data)
                    elif isinstance(data, dict) and 'visits' in data and isinstance(data['visits'], list):
                        rows.extend(data['visits'])
                    else:
                        for line in txt.splitlines():
                            line=line.strip()
                            if not line: continue
                            try: rows.append(json.loads(line))
                            except Exception: continue
                except Exception:
                    for line in txt.splitlines():
                        line=line.strip()
                        if not line: continue
                        try: rows.append(json.loads(line))
                        except Exception: continue
            except Exception as e:
                api_diag['error'] = repr(e)

    api_diag['rows_before_flatten'] = len(rows)
    # Fallback: local archives if no API rows
    fb_diag = {
        'used': False,
        'dir': None,
        'files_considered': 0,
        'files_loaded': 0,
    }
    if not rows:
        fb = fallback_dir or DEFAULT_TMP_DOWNLOADS
        fb_diag['dir'] = str(fb)
        if fb.exists():
            files = sorted(fb.glob('all-visits-*.jsonl'))
            fb_diag['files_considered'] = len(files)
            for p in files:
                try:
                    loaded_any = False
                    for line in p.read_text().splitlines():
                        line=line.strip();
                        if not line: continue
                        try:
                            rows.append(json.loads(line)); loaded_any = True
                        except Exception:
                            continue
                    if loaded_any:
                        fb_diag['files_loaded'] += 1
                except Exception:
                    continue
        fb_diag['used'] = len(rows) > 0

    # Flatten potential wrapper objects
    if rows:
        flattened: list[dict] = []
        for r in rows:
            if isinstance(r, dict) and 'visits' in r and isinstance(r['visits'], list):
                for v in r['visits']:
                    if isinstance(v, dict):
                        flattened.append(v)
            else:
                flattened.append(r)
        rows = flattened
    api_diag['rows_after_flatten'] = len(rows)

    # Derive observed date set & ts range
    observed_dates_set = set()
    earliest_ts = None
    latest_ts = None
    for r in rows:
        ts = r.get('timestamp') or r.get('event_ts') or r.get('time')
        if ts:
            dt_obj = _iso_date(str(ts))
            if dt_obj:
                observed_dates_set.add(dt_obj)
            iso = str(ts)
            if earliest_ts is None or iso < earliest_ts:
                earliest_ts = iso
            if latest_ts is None or iso > latest_ts:
                latest_ts = iso
    observed_dates = sorted(str(d) for d in observed_dates_set)
    max_dt = max(observed_dates_set) if observed_dates_set else None
    freshness_gap_days = (datetime.date.today() - max_dt).days if max_dt else None
    zero_rows_reason = None
    if len(rows) == 0:
        if api_diag['attempted'] and api_diag['error']:
            zero_rows_reason = 'api_error'
        elif api_diag['attempted'] and api_diag['status_code'] and api_diag['status_code'] == 200:
            zero_rows_reason = 'api_empty'
        else:
            zero_rows_reason = 'no_fallback_files'

    written = _write_rows_per_day(rows, out_base, 'page_count.csv', overwrite)
    return {
        'source': 'page_count',
        'mode': 'all',
        'days': len(written),
        'written': written,
        'api': api_diag,
        'fallback': fb_diag,
        'observed_dates': observed_dates,
        'earliest_ts': earliest_ts,
        'latest_ts': latest_ts,
        'freshness_gap_days': freshness_gap_days,
        'zero_rows_reason': zero_rows_reason,
    }

__all__ = ['fetch_page_count_recent', 'fetch_search_logs_snapshot', 'fetch_page_count_all', 'fetch_search_logs_file']

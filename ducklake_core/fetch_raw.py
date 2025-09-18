import os, json, pathlib, datetime, requests, csv, re
from urllib.parse import urlparse, parse_qs, unquote
from .simple_pipeline import RAW_DIR

DEFAULT_TMP_DOWNLOADS = pathlib.Path('_tmp_downloads')

PAGE_COUNT_ENV_URL = 'PAGE_COUNT_API_URL'  # Expected base URL e.g. https://example/api/page_count
PAGE_COUNT_ENV_KEY = 'PAGE_COUNT_API_KEY'  # Optional auth token

SEARCH_LOGS_ENV_URL = 'SEARCH_LOGS_API_URL'
SEARCH_LOGS_ENV_KEY = 'SEARCH_LOGS_API_KEY'


def _iso_date(ts: str):
    try:
        return datetime.datetime.fromisoformat(ts.replace('Z','+00:00')).date()
    except Exception:
        try:
            return datetime.date.fromisoformat(ts[:10])
        except Exception:
            return None


def _write_rows_per_day(rows, out_base: pathlib.Path, filename: str, overwrite: bool):
    by_day = {}
    for r in rows:
        ts = r.get('timestamp') or r.get('event_ts') or r.get('time')
        dt = _iso_date(ts) if ts else None
        if not dt:
            continue
        by_day.setdefault(dt, []).append(r)
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
    """Fetch last N days of page_count events and write raw CSV partitions.

    Strategy:
      1. If base_url provided (or via PAGE_COUNT_API_URL env) call `${base_url}?recent=days` expecting JSONL or JSON array.
      2. If request fails or no base_url, attempt to read fallback_dir JSONL files matching all-visits-*.jsonl (used for local dev).
    """
    base_url = base_url or os.getenv(PAGE_COUNT_ENV_URL)
    api_key = api_key or os.getenv(PAGE_COUNT_ENV_KEY)
    out_base = RAW_DIR / 'page_count'
    out_base.mkdir(parents=True, exist_ok=True)

    rows = []
    if base_url:
        url = f"{base_url}?recent={days}" if 'recent=' not in base_url else base_url
        headers = {'Authorization': f"Bearer {api_key}"} if api_key else {}
        try:
            resp = requests.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            txt = resp.text.strip()
            if txt.startswith('['):
                data = resp.json()
                rows.extend(data if isinstance(data, list) else [])
            else:
                for line in txt.splitlines():
                    line=line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        continue
        except Exception:
            pass  # fall back

    if not rows:
        fb = fallback_dir or DEFAULT_TMP_DOWNLOADS
        if fb.exists():
            for p in sorted(fb.glob('all-visits-*.jsonl'))[-days:]:
                try:
                    for line in p.read_text().splitlines():
                        line=line.strip()
                        if not line: continue
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            continue
                except Exception:
                    continue
    return {'source': 'page_count', 'days': days, 'written': _write_rows_per_day(rows, out_base, 'page_count.csv', overwrite)}


def fetch_search_logs_snapshot(snapshot_url: str | None = None, overwrite: bool = False, base_url: str | None = None, api_key: str | None = None):
    """Fetch search logs either from provided snapshot_url (JSONL) or SEARCH_LOGS_API_URL env.

    Writes per-day CSV partitions using timestamp/event_ts/time detection.
    """
    base_url = snapshot_url or base_url or os.getenv(SEARCH_LOGS_ENV_URL)
    api_key = api_key or os.getenv(SEARCH_LOGS_ENV_KEY)
    out_base = RAW_DIR / 'search_logs'
    out_base.mkdir(parents=True, exist_ok=True)
    rows = []
    if base_url:
        headers = {'Authorization': f"Bearer {api_key}"} if api_key else {}
        try:
            resp = requests.get(base_url, headers=headers, timeout=120)
            resp.raise_for_status()
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

    for raw_line in file_path.read_text(errors='ignore').splitlines():
        total += 1
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
    """Fetch ALL available page_count events and write raw CSV partitions.

    Strategy hierarchy:
      1. If base_url provided attempt a full export by appending `all=1` (best-effort; silently falls back).
      2. Fallback: read EVERY local JSONL archive matching all-visits-*.jsonl in fallback_dir (default _tmp_downloads).

    This is intentionally conservative and tolerant of partial failures: any unreadable
    archive is skipped. Duplicate rows across archives are not de-duplicated (assumed
    each file represents a distinct day). If multiple archives contain overlapping
    days, later files simply add more rows for that day prior to writing the CSV.
    """
    base_url = base_url or os.getenv(PAGE_COUNT_ENV_URL)
    api_key = api_key or os.getenv(PAGE_COUNT_ENV_KEY)
    out_base = RAW_DIR / 'page_count'
    out_base.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []

    # Attempt single-shot full export if API provided
    if base_url:
        # Try to append all=1 unless caller already supplied a query parameter
        full_url = base_url
        if 'all=' not in base_url:
            if '?' in base_url:
                full_url = base_url + '&all=1'
            else:
                full_url = base_url + '?all=1'
        headers = {'Authorization': f"Bearer {api_key}"} if api_key else {}
        try:
            resp = requests.get(full_url, headers=headers, timeout=180)
            resp.raise_for_status()
            txt = resp.text.strip()
            try:
                data = resp.json()
                if isinstance(data, list):
                    rows.extend(data)
                elif isinstance(data, dict) and 'visits' in data and isinstance(data['visits'], list):
                    rows.extend(data['visits'])
                else:
                    # Fallback line parsing if unexpected structure
                    for line in txt.splitlines():
                        line=line.strip()
                        if not line: continue
                        try: rows.append(json.loads(line))
                        except Exception: continue
            except Exception:
                # Line-delimited fallback
                for line in txt.splitlines():
                    line=line.strip()
                    if not line: continue
                    try: rows.append(json.loads(line))
                    except Exception: continue
        except Exception:
            # Ignore and fall back to local archives
            rows = []

    # Fallback: local archives
    if not rows:
        fb = fallback_dir or DEFAULT_TMP_DOWNLOADS
        if fb.exists():
            for p in sorted(fb.glob('all-visits-*.jsonl')):
                try:
                    for line in p.read_text().splitlines():
                        line=line.strip()
                        if not line: continue
                        try:
                            rows.append(json.loads(line))
                        except Exception:
                            continue
                except Exception:
                    continue
    written = _write_rows_per_day(rows, out_base, 'page_count.csv', overwrite)
    return {'source': 'page_count', 'mode': 'all', 'days': len(written), 'written': written}

__all__ = ['fetch_page_count_recent', 'fetch_search_logs_snapshot', 'fetch_page_count_all', 'fetch_search_logs_file']

"""Search log parsing utilities with format-specific handlers."""
from __future__ import annotations
import json
import re
import pathlib
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, parse_qs, unquote
from .config import LogFormat, QueryField, UrlField, IpField, TimestampField


def detect_log_format(file_path: pathlib.Path) -> LogFormat:
    """Detect the format of a log file by examining its content."""
    try:
        sample = file_path.read_text(encoding='utf-8', errors='ignore')[:1000]
        sample = sample.strip()

        if sample.startswith('INFO:root:') and ' - IP:' in sample and ' - Query:' in sample:
            return LogFormat.STRUCTURED_LOG
        elif sample.startswith('{') and sample.count('\n{') > 0:
            return LogFormat.JSON_LINES
        elif ',' in sample and sample.count(',') > sample.count('\n'):
            return LogFormat.CSV
        else:
            return LogFormat.PLAIN_TEXT
    except Exception:
        return LogFormat.PLAIN_TEXT


def normalize_timestamp(raw: Optional[str], assume_utc: bool = True) -> Optional[str]:
    """Normalize timestamp to ISO format."""
    if not raw:
        return None

    raw = raw.strip()

    # Space-separated date time -> T format
    if re.match(r'^20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', raw):
        return raw.replace(' ', 'T') + ('Z' if assume_utc else '')

    # Date only -> append midday
    if re.match(r'^20\d{2}-\d{2}-\d{2}$', raw):
        return raw + 'T12:00:00' + ('Z' if assume_utc else '')

    return raw


def extract_query_from_url(url_str: str) -> Optional[str]:
    """Extract query parameter from URL string."""
    try:
        if not url_str:
            return None

        # Ensure URL has scheme for proper parsing
        if '://' not in url_str:
            url_str = 'http://local' + url_str

        parsed = urlparse(url_str)
        query_params = parse_qs(parsed.query)

        for param_name in ['q', 'query', 'term', 'search', 'keyword']:
            if param_name in query_params and query_params[param_name]:
                return unquote(query_params[param_name][0])

        return None
    except Exception:
        return None


def parse_structured_log_line(line: str, assume_utc: bool = True) -> Optional[Dict[str, Any]]:
    """Parse structured log line: INFO:root:timestamp - IP: x - Query: y"""
    if not (line.startswith('INFO:root:') and ' - IP:' in line and ' - Query:' in line):
        return None

    try:
        rest = line.split('INFO:root:', 1)[1]
        ts_part, remainder = rest.split(' - IP:', 1)
        timestamp = normalize_timestamp(ts_part.strip(), assume_utc)

        ip_part, query_part = remainder.split(' - Query:', 1)
        ip = ip_part.strip()
        query = query_part.strip()

        if timestamp and query:
            return {'timestamp': timestamp, 'query': query, 'ip': ip}
    except Exception:
        pass

    return None


def parse_json_line(line: str, assume_utc: bool = True) -> Optional[Dict[str, Any]]:
    """Parse JSON line format."""
    try:
        obj = json.loads(line)
        if not isinstance(obj, dict):
            return None

        # Extract timestamp
        timestamp = None
        for field in TimestampField.all_values():
            if field in obj and obj[field]:
                timestamp = normalize_timestamp(str(obj[field]), assume_utc)
                break

        # Extract query
        query = None
        for field in QueryField.all_values():
            if field in obj and obj[field]:
                query = str(obj[field])
                break

        # If no direct query field, try to extract from URL fields
        if not query:
            for field in UrlField.all_values():
                if field in obj and obj[field]:
                    query = extract_query_from_url(str(obj[field]))
                    if query:
                        break

        # Extract IP
        ip = None
        for field in IpField.all_values():
            if field in obj and obj[field]:
                ip = str(obj[field])
                break

        if timestamp and query:
            result = {'timestamp': timestamp, 'query': query}
            if ip:
                result['ip'] = ip
            return result
    except Exception:
        pass

    return None


def parse_csv_line(line: str, assume_utc: bool = True) -> Optional[Dict[str, Any]]:
    """Parse CSV-like line format."""
    parts = [p.strip() for p in line.split(',')]
    if not parts:
        return None

    first = parts[0]
    timestamp_regex = re.compile(r'(20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:?\d{2})?)')
    date_regex = re.compile(r'^(20\d{2}-\d{2}-\d{2})$')

    # Check if first field is a timestamp
    if timestamp_regex.match(first) or date_regex.match(first):
        timestamp = normalize_timestamp(first, assume_utc)

        # Look for query in remaining fields
        query = None
        for part in parts[1:]:
            if 'query=' in part:
                query = part.split('query=', 1)[1]
                break
            elif 'q=' in part:
                query = part.split('q=', 1)[1]
                break

        # If no explicit query param, treat second column as query
        if not query and len(parts) > 1:
            query = parts[1]

        if timestamp and query:
            return {'timestamp': timestamp, 'query': query}

    return None


def parse_plain_text_line(line: str, assume_utc: bool = True) -> Optional[Dict[str, Any]]:
    """Parse plain text line using regex patterns."""
    timestamp_regex = re.compile(r'(20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:?\d{2})?)')

    # Extract timestamp
    ts_match = timestamp_regex.search(line)
    timestamp = normalize_timestamp(ts_match.group(1), assume_utc) if ts_match else None

    query = None

    # Try to extract from URL
    url_match = re.search(r'(https?://\S+)', line)
    if url_match:
        query = extract_query_from_url(url_match.group(1))

    # Try key=value tokens if no URL query found
    if not query:
        for token in line.split():
            if any(token.startswith(f'{param}=') for param in ['query', 'q', 'search']):
                query = token.split('=', 1)[1].strip('"')
                break

    if timestamp and query:
        return {'timestamp': timestamp, 'query': query}

    return None


def parse_search_logs_file(file_path: pathlib.Path, assume_utc: bool = True) -> List[Dict[str, Any]]:
    """Parse search logs file with automatic format detection."""
    if not file_path.exists():
        raise FileNotFoundError(f"Log file not found: {file_path}")

    format_type = detect_log_format(file_path)
    records = []

    total_lines = 0
    parsed_lines = 0
    skipped_no_timestamp = 0
    skipped_no_query = 0

    try:
        content = file_path.read_text(errors='ignore')
    except Exception as e:
        raise RuntimeError(f"Failed to read log file {file_path}: {e}")

    for raw_line in content.splitlines():
        total_lines += 1
        line = raw_line.strip()

        if not line:
            continue

        record = None

        # Parse based on detected format
        if format_type == LogFormat.STRUCTURED_LOG:
            record = parse_structured_log_line(line, assume_utc)
        elif format_type == LogFormat.JSON_LINES:
            record = parse_json_line(line, assume_utc)
        elif format_type == LogFormat.CSV:
            record = parse_csv_line(line, assume_utc)
        else:  # LogFormat.PLAIN_TEXT
            record = parse_plain_text_line(line, assume_utc)

        if record:
            parsed_lines += 1
            records.append(record)
        else:
            # Count skipped reasons for diagnostics
            has_timestamp = any(re.search(r'20\d{2}-\d{2}-\d{2}', line))
            if has_timestamp:
                skipped_no_query += 1
            else:
                skipped_no_timestamp += 1

    return records


def get_parse_statistics(file_path: pathlib.Path, records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get parsing statistics for diagnostics."""
    try:
        total_lines = sum(1 for _ in file_path.read_text(errors='ignore').splitlines())
    except Exception:
        total_lines = 0

    return {
        'source': 'search_logs',
        'mode': 'file',
        'format': detect_log_format(file_path).value,
        'total_lines': total_lines,
        'parsed_lines': len(records),
        'rows': len(records),
        'success_rate': len(records) / total_lines if total_lines > 0 else 0.0
    }
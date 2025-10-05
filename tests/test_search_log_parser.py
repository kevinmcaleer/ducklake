"""Tests for search log parsing functionality."""
import pytest
import tempfile
import pathlib
from ducklake_core.search_log_parser import (
    detect_log_format,
    normalize_timestamp,
    extract_query_from_url,
    parse_structured_log_line,
    parse_json_line,
    parse_csv_line,
    parse_plain_text_line,
    parse_search_logs_file,
    get_parse_statistics
)
from ducklake_core.config import LogFormat


class TestLogFormatDetection:
    """Test log format detection."""

    def test_detect_json_lines_format(self):
        """Test detection of JSON lines format."""
        content = '''{"timestamp": "2023-01-01T10:00:00Z", "query": "test"}
{"timestamp": "2023-01-01T10:01:00Z", "query": "another"}'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write(content)
            f.flush()
            result = detect_log_format(pathlib.Path(f.name))

        assert result == LogFormat.JSON_LINES

    def test_detect_structured_log_format(self):
        """Test detection of structured log format."""
        content = '''INFO:root:2023-01-01T10:00:00Z - IP: 192.168.1.1 - Query: test query
INFO:root:2023-01-01T10:01:00Z - IP: 192.168.1.2 - Query: another query'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write(content)
            f.flush()
            result = detect_log_format(pathlib.Path(f.name))

        assert result == LogFormat.STRUCTURED_LOG

    def test_detect_csv_format(self):
        """Test detection of CSV format."""
        content = '''timestamp,query,ip
2023-01-01T10:00:00Z,test query,192.168.1.1
2023-01-01T10:01:00Z,another query,192.168.1.2'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write(content)
            f.flush()
            result = detect_log_format(pathlib.Path(f.name))

        assert result == LogFormat.CSV

    def test_detect_plain_text_format(self):
        """Test detection of plain text format as fallback."""
        content = '''Some random log content without clear structure
2023-01-01T10:00:00Z query=test
Another line'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write(content)
            f.flush()
            result = detect_log_format(pathlib.Path(f.name))

        assert result == LogFormat.PLAIN_TEXT


class TestTimestampNormalization:
    """Test timestamp normalization."""

    def test_normalize_space_separated_datetime(self):
        """Test normalizing space-separated datetime."""
        result = normalize_timestamp('2023-01-01 10:00:00', assume_utc=True)
        assert result == '2023-01-01T10:00:00Z'

    def test_normalize_space_separated_datetime_no_utc(self):
        """Test normalizing space-separated datetime without UTC assumption."""
        result = normalize_timestamp('2023-01-01 10:00:00', assume_utc=False)
        assert result == '2023-01-01T10:00:00'

    def test_normalize_date_only(self):
        """Test normalizing date-only timestamp."""
        result = normalize_timestamp('2023-01-01', assume_utc=True)
        assert result == '2023-01-01T12:00:00Z'

    def test_normalize_already_iso(self):
        """Test normalizing already ISO timestamp."""
        timestamp = '2023-01-01T10:00:00Z'
        result = normalize_timestamp(timestamp)
        assert result == timestamp

    def test_normalize_empty_timestamp(self):
        """Test normalizing empty/None timestamp."""
        assert normalize_timestamp(None) is None
        assert normalize_timestamp('') is None
        assert normalize_timestamp('   ') is None


class TestQueryExtraction:
    """Test query extraction from URLs."""

    def test_extract_query_from_url_with_q(self):
        """Test extracting query using 'q' parameter."""
        url = 'https://example.com/search?q=test%20query&other=value'
        result = extract_query_from_url(url)
        assert result == 'test query'

    def test_extract_query_from_url_with_query(self):
        """Test extracting query using 'query' parameter."""
        url = 'https://example.com/search?query=search%20term'
        result = extract_query_from_url(url)
        assert result == 'search term'

    def test_extract_query_from_path_only(self):
        """Test extracting query from path without domain."""
        url = '/search?q=test+query'
        result = extract_query_from_url(url)
        assert result == 'test query'

    def test_extract_query_no_query_param(self):
        """Test URL without query parameters."""
        url = 'https://example.com/page'
        result = extract_query_from_url(url)
        assert result is None

    def test_extract_query_invalid_url(self):
        """Test invalid URL handling."""
        result = extract_query_from_url('not-a-url')
        assert result is None


class TestStructuredLogParsing:
    """Test parsing of structured log format."""

    def test_parse_structured_log_line_valid(self):
        """Test parsing valid structured log line."""
        line = 'INFO:root:2023-01-01T10:00:00Z - IP: 192.168.1.1 - Query: test query'
        result = parse_structured_log_line(line)

        assert result is not None
        assert result['timestamp'] == '2023-01-01T10:00:00Z'
        assert result['query'] == 'test query'
        assert result['ip'] == '192.168.1.1'

    def test_parse_structured_log_line_invalid(self):
        """Test parsing invalid structured log line."""
        line = 'Some random log line'
        result = parse_structured_log_line(line)
        assert result is None

    def test_parse_structured_log_line_missing_parts(self):
        """Test parsing structured log line with missing parts."""
        line = 'INFO:root:2023-01-01T10:00:00Z - IP: 192.168.1.1'
        result = parse_structured_log_line(line)
        assert result is None


class TestJSONLineParsing:
    """Test parsing of JSON line format."""

    def test_parse_json_line_valid(self):
        """Test parsing valid JSON line."""
        line = '{"timestamp": "2023-01-01T10:00:00Z", "query": "test query", "ip": "192.168.1.1"}'
        result = parse_json_line(line)

        assert result is not None
        assert result['timestamp'] == '2023-01-01T10:00:00Z'
        assert result['query'] == 'test query'
        assert result['ip'] == '192.168.1.1'

    def test_parse_json_line_with_url(self):
        """Test parsing JSON line with URL containing query."""
        line = '{"event_ts": "2023-01-01T10:00:00Z", "url": "/search?q=test+query"}'
        result = parse_json_line(line)

        assert result is not None
        assert result['timestamp'] == '2023-01-01T10:00:00Z'
        assert result['query'] == 'test query'

    def test_parse_json_line_invalid_json(self):
        """Test parsing invalid JSON."""
        line = '{"invalid": json}'
        result = parse_json_line(line)
        assert result is None

    def test_parse_json_line_missing_required_fields(self):
        """Test parsing JSON line missing required fields."""
        line = '{"other_field": "value"}'
        result = parse_json_line(line)
        assert result is None


class TestCSVLineParsing:
    """Test parsing of CSV format."""

    def test_parse_csv_line_with_timestamp_first(self):
        """Test parsing CSV line with timestamp as first field."""
        line = '2023-01-01T10:00:00Z,test query,192.168.1.1'
        result = parse_csv_line(line)

        assert result is not None
        assert result['timestamp'] == '2023-01-01T10:00:00Z'
        assert result['query'] == 'test query'

    def test_parse_csv_line_with_query_param(self):
        """Test parsing CSV line with explicit query parameter."""
        line = '2023-01-01T10:00:00Z,query=test+query,other=value'
        result = parse_csv_line(line)

        assert result is not None
        assert result['query'] == 'test+query'

    def test_parse_csv_line_invalid_timestamp(self):
        """Test parsing CSV line with invalid timestamp."""
        line = 'not-a-timestamp,query,ip'
        result = parse_csv_line(line)
        assert result is None


class TestPlainTextParsing:
    """Test parsing of plain text format."""

    def test_parse_plain_text_with_url(self):
        """Test parsing plain text line with URL."""
        line = '2023-01-01T10:00:00Z GET https://example.com/search?q=test+query HTTP/1.1'
        result = parse_plain_text_line(line)

        assert result is not None
        assert result['timestamp'] == '2023-01-01T10:00:00Z'
        assert result['query'] == 'test query'

    def test_parse_plain_text_with_key_value(self):
        """Test parsing plain text line with key=value format."""
        line = '2023-01-01T10:00:00Z user_request query=test search=keyword'
        result = parse_plain_text_line(line)

        assert result is not None
        assert result['query'] == 'test'

    def test_parse_plain_text_no_query(self):
        """Test parsing plain text line without query."""
        line = '2023-01-01T10:00:00Z some log message'
        result = parse_plain_text_line(line)
        assert result is None


class TestEndToEndParsing:
    """Test end-to-end log file parsing."""

    def test_parse_search_logs_file_json_lines(self):
        """Test parsing complete JSON lines file."""
        content = '''{"timestamp": "2023-01-01T10:00:00Z", "query": "test query"}
{"timestamp": "2023-01-01T10:01:00Z", "query": "another query"}
{"timestamp": "2023-01-01T10:02:00Z", "invalid": "line"}'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write(content)
            f.flush()
            results = parse_search_logs_file(pathlib.Path(f.name))

        # Should parse 2 valid lines, skip 1 invalid
        assert len(results) == 2
        assert results[0]['query'] == 'test query'
        assert results[1]['query'] == 'another query'

    def test_parse_search_logs_file_structured(self):
        """Test parsing complete structured log file."""
        content = '''INFO:root:2023-01-01T10:00:00Z - IP: 192.168.1.1 - Query: first query
INFO:root:2023-01-01T10:01:00Z - IP: 192.168.1.2 - Query: second query
Some invalid line'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write(content)
            f.flush()
            results = parse_search_logs_file(pathlib.Path(f.name))

        assert len(results) == 2
        assert results[0]['query'] == 'first query'
        assert results[0]['ip'] == '192.168.1.1'

    def test_parse_search_logs_file_nonexistent(self):
        """Test parsing nonexistent file."""
        with pytest.raises(FileNotFoundError):
            parse_search_logs_file(pathlib.Path('/nonexistent/file.log'))

    def test_get_parse_statistics(self):
        """Test parsing statistics generation."""
        content = '''{"timestamp": "2023-01-01T10:00:00Z", "query": "test"}
{"timestamp": "2023-01-01T10:01:00Z", "query": "another"}
invalid line'''

        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            f.write(content)
            f.flush()
            file_path = pathlib.Path(f.name)
            results = parse_search_logs_file(file_path)
            stats = get_parse_statistics(file_path, results)

        assert stats['source'] == 'search_logs'
        assert stats['mode'] == 'file'
        assert stats['format'] == 'json_lines'
        assert stats['total_lines'] == 3
        assert stats['parsed_lines'] == 2
        assert stats['rows'] == 2
        assert stats['success_rate'] == 2/3
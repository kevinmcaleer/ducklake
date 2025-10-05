"""Configuration constants and utilities for DuckLake."""
from __future__ import annotations
import yaml
from pathlib import Path
from typing import Dict
from enum import Enum


def load_sources_config(cfg_path: Path) -> Dict:
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# Core pipeline sources
SOURCES = ['page_count', 'search_logs']


class TimestampField(Enum):
    """Common timestamp field names in different formats."""
    TIMESTAMP = "timestamp"
    EVENT_TS = "event_ts"
    TIME = "time"
    TS = "ts"
    CREATED_AT = "created_at"

    @classmethod
    def all_values(cls) -> list[str]:
        return [field.value for field in cls]


class QueryField(Enum):
    """Common query field names in search logs."""
    QUERY = "query"
    Q = "q"
    TERM = "term"
    SEARCH = "search"
    KEYWORD = "keyword"

    @classmethod
    def all_values(cls) -> list[str]:
        return [field.value for field in cls]


class UrlField(Enum):
    """Common URL field names that might contain query parameters."""
    URL = "url"
    PATH = "path"
    REQUEST = "request"

    @classmethod
    def all_values(cls) -> list[str]:
        return [field.value for field in cls]


class IpField(Enum):
    """Common IP address field names."""
    IP = "ip"
    REMOTE_ADDR = "remote_addr"
    CLIENT_IP = "client_ip"

    @classmethod
    def all_values(cls) -> list[str]:
        return [field.value for field in cls]


class LogFormat(Enum):
    """Supported log file formats."""
    JSON_LINES = "json_lines"
    CSV = "csv"
    PLAIN_TEXT = "plain_text"
    STRUCTURED_LOG = "structured_log"  # e.g., INFO:root:timestamp - IP: x - Query: y


# Bot detection keywords
BOT_KEYWORDS = ['bot', 'spider', 'crawl']

# Query parameter names to search for in URLs
URL_QUERY_PARAMS = ['q', 'query', 'term', 'search', 'keyword']

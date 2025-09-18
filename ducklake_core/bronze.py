"""
Bronze ingestion utilities for DuckLake.

These functions are parameterized to avoid global state:
- conn: duckdb.DuckDBPyConnection
- lake_root: pathlib.Path pointing to the project root (where bronze/silver/gold live)
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import time
from datetime import date, datetime
from typing import Dict, Optional

import duckdb
from .utils import _sql_str, read_sql_for

try:
	import yaml  # type: ignore
except ImportError:  # pragma: no cover
	yaml = None


def sha256_file(p: pathlib.Path) -> str:
	"""Deprecated legacy bronze module stub (intentionally empty)."""

	__all__: list[str] = []
						)
						added += 1
					except duckdb.Error:
						# best-effort backfill; continue
						continue
	if added:
		print(f"Backfilled manifest for source={source_name}: {added} new file(s)")


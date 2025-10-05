"""
Bronze ingestion utilities for DuckLake.

This module contains deprecated legacy functions that are no longer used.
The functionality has been moved to simple_pipeline.py for clarity and simplicity.
"""
from __future__ import annotations
import pathlib


def sha256_file(p: pathlib.Path) -> str:
	"""Deprecated legacy bronze module stub (intentionally empty)."""
	return ""


def ingest_bronze(*args, **kwargs):
	"""Deprecated function. Use simple_pipeline.simple_refresh instead."""
	raise DeprecationWarning("Bronze layer deprecated. Use simple_pipeline.simple_refresh instead.")


def backfill_manifest_from_bronze(*args, **kwargs):
	"""Deprecated function. Use simple_pipeline.simple_refresh instead."""
	raise DeprecationWarning("Bronze layer deprecated. Use simple_pipeline.simple_refresh instead.")


__all__ = ['sha256_file', 'ingest_bronze', 'backfill_manifest_from_bronze']

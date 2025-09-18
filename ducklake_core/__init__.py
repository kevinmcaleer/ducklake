"""Simplified DuckLake core: only simple pipeline + anomaly utilities."""

from .simple_pipeline import (
    simple_refresh,
    run_simple_reports,
    validate_simple_pipeline,
    ensure_core_tables,
)
from .anomaly import detect_anomalies

__all__ = [
    "simple_refresh",
    "run_simple_reports",
    "validate_simple_pipeline",
    "ensure_core_tables",
    "detect_anomalies",
]

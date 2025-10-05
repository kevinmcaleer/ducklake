"""Pipeline timing utilities for tracking phase execution times."""
from __future__ import annotations
import time
from contextlib import contextmanager
from typing import Dict


class PipelineTimer:
    """Track execution times for different pipeline phases."""

    def __init__(self):
        self.start_time = time.time()
        self.phases: Dict[str, float] = {}

    @contextmanager
    def phase(self, name: str):
        """Context manager for timing a specific phase."""
        phase_start = time.time()
        try:
            yield
        finally:
            self.phases[f"{name}_s"] = round(time.time() - phase_start, 3)

    def get_total_time(self) -> float:
        """Get total elapsed time since timer creation."""
        return round(time.time() - self.start_time, 3)

    def get_summary(self) -> Dict[str, float]:
        """Get timing summary including total time."""
        summary = self.phases.copy()
        summary['total_s'] = self.get_total_time()
        return summary
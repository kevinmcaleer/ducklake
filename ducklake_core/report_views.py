"""Deprecated module placeholder (report views removed).

All prior analytical views are now replaced by CSV report generation inside
`simple_pipeline.run_simple_reports` and related helpers.
"""

__all__: list[str] = []

def __getattr__(name):  # pragma: no cover
    raise AttributeError("report_views removed; use simple_pipeline reports")

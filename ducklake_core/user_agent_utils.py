"""Deprecated module placeholder (user agent utils removed).

User agent parsing now occurs inline within `simple_pipeline.simple_refresh`.
"""

__all__: list[str] = []

def __getattr__(name):  # pragma: no cover
    raise AttributeError("user_agent_utils removed; enrichment is inline in simple_pipeline")

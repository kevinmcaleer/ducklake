"""Deprecated module placeholder (silver layer removed).

Silver build logic has been replaced by inline enrichment in the simplified
pipeline. Do not use this module.
"""

__all__: list[str] = []

def __getattr__(name):  # pragma: no cover
    raise AttributeError("silver layer removed; use simple_pipeline.simple_refresh")

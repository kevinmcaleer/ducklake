"""Deprecated module placeholder.

This legacy Gold layer module has been removed in favor of the simplified
`simple_pipeline` implementation. Importing or using any prior functions is
no longer supported.
"""

__all__: list[str] = []

def __getattr__(name):  # pragma: no cover - defensive
    raise AttributeError("gold layer removed; use simple_pipeline outputs instead")

"""Deprecated module placeholder (manifest removed).

Previous manifest-based orchestration has been retired. The simplified pipeline
discovers raw files directly under `data/raw/<source>/dt=YYYY-MM-DD/`.
"""

__all__: list[str] = []

def __getattr__(name):  # pragma: no cover
    raise AttributeError("manifest module removed; simple_pipeline performs discovery implicitly")

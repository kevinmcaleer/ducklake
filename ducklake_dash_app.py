"""Deprecated: use dashboard.py instead.

This file kept as a compatibility stub. Running it will import and launch the
unified dashboard defined in `dashboard.py`.
"""
from dashboard import main as _main  # type: ignore

if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(_main())

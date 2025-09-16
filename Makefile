PY=python3

.PHONY: help simple-refresh validate bootstrap-raw fast-bootstrap-lake cleanup-manifest refresh reports test

help:
	@echo "Available targets:"
	@echo "  simple-refresh         - Run simplified pipeline (raw->parquet->aggregates->reports)"
	@echo "  validate               - Validate simplified pipeline objects"
	@echo "  bootstrap-raw          - Export silver tables to raw CSV layout"
	@echo "  fast-bootstrap-lake    - Export silver tables directly to lake parquet partitions"
	@echo "  cleanup-manifest       - Remove stale + obsolete manifest entries"
	@echo "  refresh                - Run legacy full refresh (bronze->silver->gold->reports)"
	@echo "  reports                - Run reports-only (legacy views + reports.sql)"
	@echo "  test                   - Run pytest suite"

simple-refresh:
	$(PY) intake.py simple-refresh

validate:
	$(PY) intake.py simple-validate

bootstrap-raw:
	$(PY) intake.py bootstrap-raw

fast-bootstrap-lake:
	$(PY) intake.py fast-bootstrap-lake

cleanup-manifest:
	$(PY) intake.py cleanup-manifest

refresh:
	$(PY) intake.py refresh

reports:
	$(PY) intake.py reports-only

test:
	pytest -q

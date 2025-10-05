#!/bin/bash
# Export contentlake.ducklake to reports.duckdb for Metabase
# This uses DuckDB 1.3.1 for compatibility with Metabase DuckDB driver 0.4.1

set -e

echo "Exporting to Metabase-compatible format (DuckDB 1.3.1)..."

# Activate virtual environment with DuckDB 1.3.1
source /tmp/duckdb131_env/bin/activate

# Run export
python3 run_refresh.py --db contentlake.ducklake export --output reports.duckdb

# Deactivate
deactivate

echo ""
echo "✓ Export complete!"
echo "  In Metabase, use database path: /data/reports.duckdb"

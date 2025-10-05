"""Tests for database utility functions."""
import pytest
import duckdb
from ducklake_core.database_utils import (
    get_table_columns,
    safe_scalar,
    table_exists,
    find_timestamp_column,
    create_coalesce_expression
)
from ducklake_core.exceptions import DatabaseOperationError


@pytest.fixture
def test_conn():
    """Create test database connection."""
    conn = duckdb.connect(':memory:')

    # Create test table
    conn.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            name VARCHAR,
            timestamp TIMESTAMP,
            event_ts TIMESTAMP,
            created_at DATE
        )
    """)

    # Insert test data
    conn.execute("""
        INSERT INTO test_table VALUES
        (1, 'test1', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '2023-01-01'),
        (2, 'test2', '2023-01-02 11:00:00', '2023-01-02 11:00:00', '2023-01-02')
    """)

    return conn


class TestDatabaseUtils:
    """Test database utility functions."""

    def test_get_table_columns_success(self, test_conn):
        """Test getting table columns for existing table."""
        columns = get_table_columns(test_conn, 'test_table')
        expected_columns = ['id', 'name', 'timestamp', 'event_ts', 'created_at']
        assert set(columns) == set(expected_columns)

    def test_get_table_columns_nonexistent_table(self, test_conn):
        """Test getting columns for nonexistent table."""
        columns = get_table_columns(test_conn, 'nonexistent_table')
        assert columns == []

    def test_safe_scalar_success(self, test_conn):
        """Test safe scalar query execution."""
        result = safe_scalar(test_conn, "SELECT COUNT(*) FROM test_table")
        assert result == 2

    def test_safe_scalar_with_params(self, test_conn):
        """Test safe scalar with parameters."""
        result = safe_scalar(test_conn, "SELECT name FROM test_table WHERE id = ?", [1])
        assert result == 'test1'

    def test_safe_scalar_empty_result(self, test_conn):
        """Test safe scalar with empty result."""
        result = safe_scalar(test_conn, "SELECT name FROM test_table WHERE id = 999")
        assert result is None

    def test_safe_scalar_invalid_query(self, test_conn):
        """Test safe scalar with invalid query."""
        result = safe_scalar(test_conn, "SELECT * FROM nonexistent_table")
        assert result is None

    def test_table_exists_true(self, test_conn):
        """Test table existence check for existing table."""
        assert table_exists(test_conn, 'test_table') is True

    def test_table_exists_false(self, test_conn):
        """Test table existence check for nonexistent table."""
        assert table_exists(test_conn, 'nonexistent_table') is False

    def test_find_timestamp_column_found(self, test_conn):
        """Test finding timestamp column when it exists."""
        result = find_timestamp_column(test_conn, 'test_table')
        # Should find 'timestamp' as it's first in the enum order
        assert result == 'timestamp'

    def test_find_timestamp_column_not_found(self, test_conn):
        """Test finding timestamp column when none exist."""
        # Create table without timestamp columns
        test_conn.execute("CREATE TABLE no_ts_table (id INTEGER, name VARCHAR)")
        result = find_timestamp_column(test_conn, 'no_ts_table')
        assert result is None

    def test_find_timestamp_column_nonexistent_table(self, test_conn):
        """Test finding timestamp column for nonexistent table."""
        result = find_timestamp_column(test_conn, 'nonexistent_table')
        assert result is None

    def test_create_coalesce_expression_found(self):
        """Test creating COALESCE expression when columns are found."""
        columns = ['id', 'timestamp', 'event_ts', 'name']
        candidates = ['timestamp', 'event_ts', 'time']

        result = create_coalesce_expression(columns, candidates)
        assert result == 'COALESCE(timestamp, event_ts)'

    def test_create_coalesce_expression_partial_match(self):
        """Test creating COALESCE expression with partial matches."""
        columns = ['id', 'event_ts', 'name']
        candidates = ['timestamp', 'event_ts', 'time']

        result = create_coalesce_expression(columns, candidates)
        assert result == 'COALESCE(event_ts)'

    def test_create_coalesce_expression_no_match(self):
        """Test creating COALESCE expression when no columns match."""
        columns = ['id', 'name', 'other']
        candidates = ['timestamp', 'event_ts', 'time']

        result = create_coalesce_expression(columns, candidates)
        assert result is None

    def test_create_coalesce_expression_empty_columns(self):
        """Test creating COALESCE expression with empty columns."""
        columns = []
        candidates = ['timestamp', 'event_ts']

        result = create_coalesce_expression(columns, candidates)
        assert result is None

    def test_create_coalesce_expression_empty_candidates(self):
        """Test creating COALESCE expression with empty candidates."""
        columns = ['timestamp', 'id']
        candidates = []

        result = create_coalesce_expression(columns, candidates)
        assert result is None


class TestDatabaseUtilsIntegration:
    """Integration tests for database utilities."""

    def test_safe_scalar_with_timestamp_queries(self, test_conn):
        """Test safe scalar with timestamp queries."""
        # Test max date query
        result = safe_scalar(test_conn, "SELECT MAX(DATE(timestamp)) FROM test_table")
        assert result is not None

        # Test count with date filter
        result = safe_scalar(
            test_conn,
            "SELECT COUNT(*) FROM test_table WHERE DATE(timestamp) > ?",
            ['2023-01-01']
        )
        assert result == 1

    def test_column_detection_workflow(self, test_conn):
        """Test complete column detection workflow."""
        # Get columns
        columns = get_table_columns(test_conn, 'test_table')
        assert len(columns) > 0

        # Find timestamp column
        ts_col = find_timestamp_column(test_conn, 'test_table')
        assert ts_col in columns

        # Create COALESCE expression
        from ducklake_core.config import TimestampField
        coalesce_expr = create_coalesce_expression(columns, TimestampField.all_values())
        assert coalesce_expr is not None
        assert ts_col in coalesce_expr

    def test_error_handling_graceful_degradation(self, test_conn):
        """Test that functions degrade gracefully on errors."""
        # Close connection to simulate database errors
        test_conn.close()

        # All functions should handle the error gracefully
        assert get_table_columns(test_conn, 'test_table') == []
        assert safe_scalar(test_conn, "SELECT 1") is None
        assert table_exists(test_conn, 'test_table') is False
        assert find_timestamp_column(test_conn, 'test_table') is None
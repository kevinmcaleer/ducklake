"""Tests for refactored simple_pipeline functions."""
import pytest
import tempfile
import pathlib
import duckdb
import pandas as pd
from ducklake_core.simple_pipeline import (
    ensure_core_tables,
    is_already_processed,
    get_partition_path,
    file_id,
    ingest_new_files,
    update_page_views_daily,
    update_searches_daily,
    enrich_page_count_data,
    build_refresh_summary,
    simple_refresh
)
from ducklake_core.pipeline_timer import PipelineTimer
from ducklake_core.exceptions import DataIngestionError, EnrichmentError


@pytest.fixture
def test_conn():
    """Create test database connection with schema."""
    conn = duckdb.connect(':memory:')
    ensure_core_tables(conn)
    return conn


@pytest.fixture
def sample_raw_data(tmp_path):
    """Create sample raw data files."""
    # Create directory structure
    raw_dir = tmp_path / 'data' / 'raw'
    page_count_dir = raw_dir / 'page_count' / 'dt=2023-01-01'
    page_count_dir.mkdir(parents=True)

    # Create sample CSV
    csv_file = page_count_dir / 'sample.csv'
    csv_file.write_text('''timestamp,ip,url,user_agent
2023-01-01T10:00:00Z,192.168.1.1,/home,Mozilla/5.0 (Windows NT 10.0; Win64; x64)
2023-01-01T10:01:00Z,192.168.1.2,/about,Mozilla/5.0 (iPhone; CPU iPhone OS 14_6)
''')

    return raw_dir


class TestCoreTableCreation:
    """Test core table creation."""

    def test_ensure_core_tables(self):
        """Test that core tables are created."""
        conn = duckdb.connect(':memory:')
        ensure_core_tables(conn)

        # Check that all tables exist
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        expected_tables = ['processed_files', 'page_views_daily', 'searches_daily', 'ingestion_state']

        for table in expected_tables:
            assert table in tables

    def test_ensure_core_tables_idempotent(self, test_conn):
        """Test that ensuring tables multiple times is safe."""
        # Should not raise error
        ensure_core_tables(test_conn)
        ensure_core_tables(test_conn)

        # Tables should still exist
        tables = [r[0] for r in test_conn.execute("SHOW TABLES").fetchall()]
        assert 'processed_files' in tables


class TestFileProcessing:
    """Test file processing functions."""

    def test_file_id_generation(self, tmp_path):
        """Test file ID generation."""
        test_file = tmp_path / 'test.csv'
        test_file.write_text('test content')

        file_id_1 = file_id(test_file)
        assert isinstance(file_id_1, str)
        assert len(file_id_1) == 64  # SHA256 hex digest

        # Same file should generate same ID
        file_id_2 = file_id(test_file)
        assert file_id_1 == file_id_2

    def test_file_id_different_content(self, tmp_path):
        """Test that different content generates different file ID."""
        file1 = tmp_path / 'test1.csv'
        file2 = tmp_path / 'test2.csv'

        file1.write_text('content 1')
        file2.write_text('content 2')

        assert file_id(file1) != file_id(file2)

    def test_is_already_processed_false(self, test_conn, tmp_path):
        """Test file not already processed."""
        test_file = tmp_path / 'new_file.csv'
        test_file.write_text('new content')

        assert is_already_processed(test_conn, test_file) is False

    def test_is_already_processed_true(self, test_conn, tmp_path):
        """Test file already processed."""
        test_file = tmp_path / 'processed_file.csv'
        test_file.write_text('processed content')

        # Mark as processed
        fid = file_id(test_file)
        test_conn.execute(
            "INSERT INTO processed_files VALUES (?, ?, ?, ?, ?, current_timestamp)",
            ['test_source', '2023-01-01', str(test_file), fid, 100]
        )

        assert is_already_processed(test_conn, test_file) is True

    def test_get_partition_path(self, tmp_path):
        """Test partition path generation."""
        from datetime import date
        from ducklake_core.simple_pipeline import LAKE_DIR

        # Mock LAKE_DIR for testing
        import ducklake_core.simple_pipeline as sp
        original_lake_dir = sp.LAKE_DIR
        sp.LAKE_DIR = tmp_path

        try:
            path = get_partition_path('test_source', date(2023, 1, 1))
            expected = tmp_path / 'test_source' / 'dt=2023-01-01.parquet'
            assert path == expected

            # Directory should be created
            assert path.parent.exists()
        finally:
            sp.LAKE_DIR = original_lake_dir


class TestDataAggregation:
    """Test data aggregation functions."""

    def test_update_page_views_daily_empty(self, test_conn):
        """Test page views update with no data."""
        # Create empty lake_page_count view
        test_conn.execute("CREATE VIEW lake_page_count AS SELECT * FROM (SELECT NULL WHERE FALSE)")

        update_page_views_daily(test_conn)

        # Should not error, but no data should be inserted
        count = test_conn.execute("SELECT COUNT(*) FROM page_views_daily").fetchone()[0]
        assert count == 0

    def test_update_page_views_daily_with_data(self, test_conn):
        """Test page views update with actual data."""
        # Create test data
        test_conn.execute("""
            CREATE TABLE test_page_data AS SELECT
                '2023-01-01T10:00:00Z'::TIMESTAMP as timestamp,
                '192.168.1.1' as ip,
                '/home' as url
            UNION ALL SELECT
                '2023-01-01T10:01:00Z'::TIMESTAMP,
                '192.168.1.2',
                '/about'
        """)

        test_conn.execute("CREATE VIEW lake_page_count AS SELECT * FROM test_page_data")

        update_page_views_daily(test_conn)

        # Should have one day of data
        result = test_conn.execute("SELECT dt, views, uniq_ips FROM page_views_daily").fetchone()
        assert result[0] == '2023-01-01'  # Date
        assert result[1] == 2  # Views
        assert result[2] == 2  # Unique IPs

    def test_update_searches_daily_with_data(self, test_conn):
        """Test searches update with actual data."""
        # Create test search data
        test_conn.execute("""
            CREATE TABLE test_search_data AS SELECT
                '2023-01-01T10:00:00Z'::TIMESTAMP as timestamp,
                'python programming' as query,
                '192.168.1.1' as ip
            UNION ALL SELECT
                '2023-01-01T10:01:00Z'::TIMESTAMP,
                'PYTHON PROGRAMMING',  -- Test normalization
                '192.168.1.2'
            UNION ALL SELECT
                '2023-01-01T10:02:00Z'::TIMESTAMP,
                'null',  -- Should be filtered out
                '192.168.1.3'
        """)

        test_conn.execute("CREATE VIEW lake_search_logs AS SELECT * FROM test_search_data")

        update_searches_daily(test_conn)

        # Should have normalized the query and filtered out 'null'
        results = test_conn.execute("SELECT dt, query, cnt FROM searches_daily ORDER BY query").fetchall()
        assert len(results) == 1
        assert results[0][0] == '2023-01-01'
        assert results[0][1] == 'python programming'  # Normalized
        assert results[0][2] == 2  # Count


class TestDataEnrichment:
    """Test data enrichment functions."""

    def test_enrich_page_count_data_no_user_agent_column(self, test_conn):
        """Test enrichment when no user_agent column exists."""
        # Create test data without user_agent
        test_conn.execute("CREATE VIEW lake_page_count AS SELECT '192.168.1.1' as ip, '/home' as url")

        enrich_page_count_data(test_conn)

        # Should create view with empty enrichment columns
        columns = [r[0] for r in test_conn.execute("DESCRIBE silver_page_count").fetchall()]
        assert 'agent_type' in columns
        assert 'is_bot' in columns

    def test_enrich_page_count_data_empty_data(self, test_conn):
        """Test enrichment with empty data."""
        # Create empty view with user_agent column
        test_conn.execute("""
            CREATE VIEW lake_page_count AS
            SELECT CAST(NULL AS VARCHAR) as user_agent, CAST(NULL AS VARCHAR) as ip
            WHERE FALSE
        """)

        enrich_page_count_data(test_conn)

        # Should create empty view with enrichment columns
        columns = [r[0] for r in test_conn.execute("DESCRIBE silver_page_count").fetchall()]
        assert 'agent_type' in columns


class TestPipelineIntegration:
    """Test complete pipeline integration."""

    def test_build_refresh_summary(self, test_conn):
        """Test building refresh summary."""
        timer = PipelineTimer()
        validation = {'test_check': True}
        anomalies = {'series': {'test_series': {'anomalies': [1, 2, 3]}}}

        summary = build_refresh_summary(test_conn, 5, timer, validation, anomalies)

        assert summary['new_files'] == 5
        assert summary['validation'] == validation
        assert summary['anomalies']['test_series'] == 3  # Count of anomalies
        assert 'timings' in summary

    def test_simple_refresh_empty_pipeline(self):
        """Test simple refresh with empty pipeline."""
        conn = duckdb.connect(':memory:')
        result = simple_refresh(conn)

        # Should complete without error
        assert 'new_files' in result
        assert 'timings' in result
        assert 'validation' in result
        assert result['new_files'] == 0  # No files to process

    def test_simple_refresh_timing(self):
        """Test that simple refresh includes timing information."""
        conn = duckdb.connect(':memory:')
        result = simple_refresh(conn)

        timings = result['timings']
        expected_phases = ['ingest_s', 'lake_views_s', 'silver_enrich_s', 'aggregates_s', 'reports_s', 'validation_s', 'anomaly_s', 'total_s']

        for phase in expected_phases:
            assert phase in timings
            assert isinstance(timings[phase], (int, float))
            assert timings[phase] >= 0


class TestErrorHandling:
    """Test error handling in refactored functions."""

    def test_enrichment_error_handling(self, test_conn):
        """Test that enrichment errors are handled gracefully."""
        # Create problematic data that might cause enrichment to fail
        test_conn.execute("CREATE VIEW lake_page_count AS SELECT 'invalid' as user_agent")

        # Should not raise unhandled exception
        try:
            enrich_page_count_data(test_conn)
        except EnrichmentError:
            # EnrichmentError is expected and should be caught by caller
            pass

    def test_simple_refresh_handles_enrichment_errors(self, test_conn):
        """Test that simple_refresh handles enrichment errors gracefully."""
        # Even with potential enrichment issues, pipeline should complete
        result = simple_refresh(test_conn)

        # Should still return valid result structure
        assert 'new_files' in result
        assert 'timings' in result
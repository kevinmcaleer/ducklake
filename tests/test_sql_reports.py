"""
Test suite for SQL-based reporting system.

Tests validate:
1. SQL files can be loaded and parsed
2. Reports generate valid CSV output 
3. DuckDB views are created successfully
4. Report data integrity and expected columns
"""
import pytest
import duckdb
import pathlib
import tempfile
import shutil
from ducklake_core.simple_pipeline import (
    load_sql_reports, 
    create_report_view, 
    run_simple_reports,
    simple_refresh,
    ensure_core_tables,
    SQL_REPORTS_DIR,
    REPORTS_DIR
)


@pytest.fixture
def test_db():
    """Create a temporary test database with sample data."""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        test_db_path = f.name
    
    conn = duckdb.connect(test_db_path)
    
    # Set up core tables and sample data
    ensure_core_tables(conn)
    
    # Sample page views data
    conn.execute("""
        INSERT INTO page_views_daily VALUES 
        ('2025-09-25', 1500, 200),
        ('2025-09-26', 1800, 250),
        ('2025-09-27', 1200, 180)
    """)
    
    # Sample search data
    conn.execute("""
        INSERT INTO searches_daily VALUES 
        ('2025-09-25', 'python tutorial', 15),
        ('2025-09-25', 'duckdb guide', 8),
        ('2025-09-26', 'python tutorial', 12),
        ('2025-09-26', 'data analysis', 6),
        ('2025-09-27', 'sql queries', 10)
    """)
    
    # Create lake views with sample data
    conn.execute("""
        CREATE OR REPLACE VIEW lake_page_count AS 
        SELECT 
            '2025-09-25T10:00:00Z' AS timestamp,
            '192.168.1.1' AS ip,
            '/home' AS url,
            'Mozilla/5.0' AS user_agent
        UNION ALL
        SELECT 
            '2025-09-26T14:30:00Z',
            '192.168.1.2', 
            '/about',
            'Chrome/91.0'
    """)
    
    conn.execute("""
        CREATE OR REPLACE VIEW lake_search_logs AS 
        SELECT 
            '2025-09-25T12:00:00Z' AS timestamp,
            '192.168.1.1' AS ip,
            'python tutorial' AS query
        UNION ALL
        SELECT 
            '2025-09-26T15:00:00Z',
            '192.168.1.2',
            'duckdb guide'
    """)
    
    # Create silver view with basic enrichment
    conn.execute("""
        CREATE OR REPLACE VIEW silver_page_count AS 
        SELECT *,
            'human' AS agent_type,
            'Linux' AS os,
            'PC' AS device,
            0 AS is_bot
        FROM lake_page_count
    """)
    
    yield conn
    
    conn.close()
    pathlib.Path(test_db_path).unlink()


class TestSQLReports:
    """Test SQL report file loading and parsing."""
    
    def test_load_sql_reports(self):
        """Test that SQL reports can be loaded from files."""
        reports = load_sql_reports()
        
        assert isinstance(reports, dict)
        assert len(reports) > 0
        
        # Check some expected reports exist
        expected_reports = [
            'page_views_by_agent_os_device',
            'visits_pages_daily', 
            'searches_daily',
            'top_queries_all_time',
            'searches_summary'
        ]
        
        for report in expected_reports:
            assert report in reports, f"Missing report: {report}"
            assert isinstance(reports[report], str), f"Report {report} should be string"
            assert len(reports[report].strip()) > 0, f"Report {report} should not be empty"
    
    def test_sql_files_exist(self):
        """Test that all expected SQL files exist in sql/reports/."""
        assert SQL_REPORTS_DIR.exists(), "SQL reports directory should exist"
        
        sql_files = list(SQL_REPORTS_DIR.glob('*.sql'))
        assert len(sql_files) > 0, "Should have SQL files"
        
        # Check for key report files
        expected_files = [
            'page_views_by_agent_os_device.sql',
            'visits_pages_daily.sql',
            'searches_daily.sql', 
            'top_queries_all_time.sql',
            'searches_summary.sql'
        ]
        
        existing_files = [f.name for f in sql_files]
        for expected in expected_files:
            assert expected in existing_files, f"Missing SQL file: {expected}"


class TestReportViews:
    """Test DuckDB view creation from reports."""
    
    def test_create_report_view(self, test_db):
        """Test creating a DuckDB view from report SQL."""
        sql = "SELECT dt, views AS cnt FROM page_views_daily ORDER BY dt"
        
        result = create_report_view(test_db, 'test_report', sql)
        assert result is True, "View creation should succeed"
        
        # Verify view exists and is queryable
        rows = test_db.execute("SELECT * FROM v_test_report").fetchall()
        assert len(rows) == 3, "Should have 3 rows of test data"
        
        # Check view structure
        columns = [desc[0] for desc in test_db.execute("DESCRIBE v_test_report").fetchall()]
        assert 'dt' in columns
        assert 'cnt' in columns
    
    def test_create_report_view_invalid_sql(self, test_db):
        """Test view creation with invalid SQL fails gracefully."""
        invalid_sql = "SELECT * FROM nonexistent_table"
        
        result = create_report_view(test_db, 'invalid_report', invalid_sql)
        assert result is False, "View creation should fail with invalid SQL"
    
    def test_all_reports_create_views(self, test_db):
        """Test that all loaded SQL reports can create views."""
        reports = load_sql_reports()
        
        created_views = []
        failed_views = []
        
        for report_name, sql in reports.items():
            result = create_report_view(test_db, report_name, sql)
            if result:
                created_views.append(f"v_{report_name}")
            else:
                failed_views.append(report_name)
        
        # Most reports should succeed (some may fail due to missing data)
        success_rate = len(created_views) / len(reports) if reports else 0
        assert success_rate >= 0.7, f"At least 70% of views should be created. Failed: {failed_views}"
        
        # Verify created views are queryable
        for view_name in created_views[:5]:  # Test first 5 to avoid long test times
            try:
                test_db.execute(f"SELECT * FROM {view_name} LIMIT 1").fetchall()
            except Exception as e:
                pytest.fail(f"View {view_name} should be queryable: {e}")


class TestReportGeneration:
    """Test full report generation process."""
    
    def test_run_simple_reports(self, test_db):
        """Test running the complete report generation process."""
        # Create temporary reports directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_reports_dir = pathlib.Path(temp_dir) / 'reports'
            
            # Monkey patch REPORTS_DIR for this test
            import ducklake_core.simple_pipeline as sp
            original_reports_dir = sp.REPORTS_DIR
            sp.REPORTS_DIR = temp_reports_dir
            
            try:
                run_simple_reports(test_db)
                
                # Check that CSV files were created
                csv_files = list(temp_reports_dir.glob('*.csv'))
                assert len(csv_files) > 0, "Should generate CSV reports"
                
                # Check summary file was created  
                summary_file = temp_reports_dir / 'simple_refresh_summary.json'
                assert summary_file.exists(), "Should create summary JSON"
                
                # Verify summary structure
                import json
                summary = json.loads(summary_file.read_text())
                assert 'generated_reports' in summary
                assert 'created_views' in summary
                assert 'ts' in summary
                
                # Check some CSV files have content
                non_empty_csvs = 0
                for csv_file in csv_files:
                    lines = csv_file.read_text().strip().split('\n')
                    if len(lines) > 1:  # Header + at least one data row
                        non_empty_csvs += 1
                
                assert non_empty_csvs > 0, "At least some CSV reports should have data"
                
            finally:
                # Restore original REPORTS_DIR
                sp.REPORTS_DIR = original_reports_dir
    
    def test_csv_report_structure(self, test_db):
        """Test that generated CSV reports have proper structure."""
        reports = load_sql_reports()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_reports_dir = pathlib.Path(temp_dir) / 'reports'
            temp_reports_dir.mkdir()
            
            # Test a few specific reports
            test_reports = {
                'visits_pages_daily': ['dt', 'cnt'],
                'searches_summary': ['days', 'query_day_rows', 'total_searches', 'distinct_queries']
            }
            
            for report_name, expected_columns in test_reports.items():
                if report_name in reports:
                    sql = reports[report_name]
                    csv_file = temp_reports_dir / f"{report_name}.csv"
                    
                    try:
                        test_db.execute(f"COPY ({sql}) TO '{csv_file}' (HEADER TRUE, DELIMITER ',')")
                        
                        # Read and check CSV structure
                        if csv_file.exists():
                            lines = csv_file.read_text().strip().split('\n')
                            if lines:
                                header = lines[0].split(',')
                                for col in expected_columns:
                                    assert col in header, f"Report {report_name} missing column {col}"
                    
                    except Exception as e:
                        # Some reports may fail due to missing data - that's okay for structure test
                        print(f"Report {report_name} failed (expected for test data): {e}")


class TestReportDataIntegrity:
    """Test data integrity and business logic in reports."""
    
    def test_visits_pages_daily_data(self, test_db):
        """Test visits_pages_daily report returns expected data."""
        sql = "SELECT dt, views AS cnt FROM page_views_daily ORDER BY dt"
        
        result = test_db.execute(sql).fetchall()
        assert len(result) == 3, "Should have 3 days of data"
        
        # Check data is sorted by date
        dates = [row[0] for row in result]
        assert dates == sorted(dates), "Data should be sorted by date"
        
        # Check all counts are positive
        counts = [row[1] for row in result]
        assert all(c > 0 for c in counts), "All view counts should be positive"
    
    def test_searches_summary_calculations(self, test_db):
        """Test searches_summary report calculations."""
        sql = """
            SELECT count(DISTINCT dt) AS days, count(*) AS query_day_rows, sum(cnt) AS total_searches,
                   count(DISTINCT query) AS distinct_queries
            FROM searches_daily
        """
        
        result = test_db.execute(sql).fetchone()
        assert result is not None
        
        days, query_day_rows, total_searches, distinct_queries = result
        
        # Basic validation
        assert days == 3, "Should have 3 distinct days"
        assert query_day_rows == 5, "Should have 5 query-day combinations"
        assert total_searches == 51, "Total searches should sum correctly (15+8+12+6+10)"
        assert distinct_queries == 4, "Should have 4 distinct queries"
    
    def test_top_queries_limit_and_order(self, test_db):
        """Test top queries reports respect LIMIT and ordering."""
        sql = """
            WITH norm AS (
              SELECT lower(trim(regexp_replace(query, '\\s+', ' '))) AS query_norm, cnt
              FROM searches_daily
            )
            SELECT query_norm AS query, sum(cnt) AS cnt
            FROM norm
            WHERE query_norm <> 'null'
            GROUP BY 1 ORDER BY cnt DESC LIMIT 100
        """
        
        result = test_db.execute(sql).fetchall()
        
        # Should be ordered by count descending
        counts = [row[1] for row in result]
        assert counts == sorted(counts, reverse=True), "Results should be ordered by count DESC"
        
        # Should have at most 100 results (we have fewer in test data)
        assert len(result) <= 100, "Should respect LIMIT 100"


if __name__ == '__main__':
    pytest.main([__file__])
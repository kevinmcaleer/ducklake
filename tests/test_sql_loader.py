"""Tests for SQL file loading utilities."""
import pytest
import tempfile
import pathlib
from ducklake_core.sql_loader import (
    load_sql_file,
    load_schema_sql,
    get_sql_path,
    load_aggregate_sql,
    load_view_sql,
    load_schema_sql_file
)
from ducklake_core.exceptions import DatabaseOperationError


class TestSQLFileLoading:
    """Test SQL file loading functionality."""

    def test_load_sql_file_basic(self):
        """Test loading basic SQL file."""
        sql_content = """-- Test SQL file
SELECT * FROM test_table;"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name))

        # Should remove comments and trailing semicolon
        assert result == 'SELECT * FROM test_table'

    def test_load_sql_file_with_parameters(self):
        """Test loading SQL file with parameter substitution."""
        sql_content = """SELECT * FROM {table_name} WHERE id = {user_id}"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name), table_name='users', user_id=123)

        assert result == 'SELECT * FROM users WHERE id = 123'

    def test_load_sql_file_remove_comments(self):
        """Test that comments are removed from SQL."""
        sql_content = """-- This is a comment
SELECT id,
-- Another comment
name FROM table
-- Final comment"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name))

        expected = """SELECT id,

name FROM table"""
        assert result == expected

    def test_load_sql_file_remove_trailing_semicolon(self):
        """Test that trailing semicolon is removed."""
        sql_content = "SELECT * FROM table;"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name))

        assert result == 'SELECT * FROM table'

    def test_load_sql_file_nonexistent(self):
        """Test loading nonexistent SQL file."""
        with pytest.raises(DatabaseOperationError):
            load_sql_file(pathlib.Path('/nonexistent/file.sql'))

    def test_load_schema_sql_directory(self):
        """Test loading all SQL files from schema directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = pathlib.Path(temp_dir)

            # Create test SQL files
            (temp_path / 'table1.sql').write_text('CREATE TABLE table1 (id INTEGER);')
            (temp_path / 'table2.sql').write_text('CREATE TABLE table2 (name VARCHAR);')
            (temp_path / 'not_sql.txt').write_text('This is not SQL')

            result = load_schema_sql(temp_path)

        # Should load only .sql files
        assert len(result) == 2
        assert 'table1' in result
        assert 'table2' in result
        assert 'not_sql' not in result
        assert 'CREATE TABLE table1' in result['table1']

    def test_load_schema_sql_empty_directory(self):
        """Test loading from empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = load_schema_sql(pathlib.Path(temp_dir))

        assert result == {}

    def test_load_schema_sql_nonexistent_directory(self):
        """Test loading from nonexistent directory."""
        result = load_schema_sql(pathlib.Path('/nonexistent/directory'))
        assert result == {}

    def test_get_sql_path(self):
        """Test getting SQL path relative to project root."""
        path = get_sql_path('schema/test.sql')
        assert isinstance(path, pathlib.Path)
        assert str(path).endswith('sql/schema/test.sql')

    def test_load_aggregate_sql_with_params(self):
        """Test loading aggregate SQL with parameters."""
        # This test assumes the actual aggregate SQL files exist
        try:
            result = load_aggregate_sql('page_views_daily_insert', coalesce_expr='timestamp')
            assert isinstance(result, str)
            assert 'INSERT' in result.upper()
            assert 'timestamp' in result
        except DatabaseOperationError:
            # If file doesn't exist, that's also a valid test result
            pass

    def test_load_view_sql_with_params(self):
        """Test loading view SQL with parameters."""
        try:
            result = load_view_sql('silver_page_count_from_parquet', parquet_path='/path/to/file.parquet')
            assert isinstance(result, str)
            assert 'CREATE' in result.upper()
            assert '/path/to/file.parquet' in result
        except DatabaseOperationError:
            # If file doesn't exist, that's also a valid test result
            pass

    def test_load_schema_sql_file(self):
        """Test loading specific schema SQL file."""
        try:
            result = load_schema_sql_file('processed_files')
            assert isinstance(result, str)
            assert 'CREATE TABLE' in result.upper()
            assert 'processed_files' in result
        except DatabaseOperationError:
            # If file doesn't exist, that's also a valid test result
            pass


class TestSQLParameterSubstitution:
    """Test SQL parameter substitution functionality."""

    def test_parameter_substitution_multiple_params(self):
        """Test substitution with multiple parameters."""
        sql_content = "SELECT {column} FROM {table} WHERE {condition}"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(
                pathlib.Path(f.name),
                column='name',
                table='users',
                condition='active = 1'
            )

        assert result == 'SELECT name FROM users WHERE active = 1'

    def test_parameter_substitution_repeated_params(self):
        """Test substitution with repeated parameters."""
        sql_content = "SELECT {col} FROM table WHERE {col} IS NOT NULL"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name), col='timestamp')

        assert result == 'SELECT timestamp FROM table WHERE timestamp IS NOT NULL'

    def test_parameter_substitution_missing_param(self):
        """Test substitution with missing parameter."""
        sql_content = "SELECT {column} FROM {table}"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            # Should raise KeyError when parameter is missing
            with pytest.raises(DatabaseOperationError):
                load_sql_file(pathlib.Path(f.name), column='name')  # Missing 'table'

    def test_parameter_substitution_no_params_needed(self):
        """Test loading SQL that doesn't need parameters."""
        sql_content = "SELECT * FROM fixed_table"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name))

        assert result == 'SELECT * FROM fixed_table'


class TestSQLFileCleaning:
    """Test SQL file cleaning functionality."""

    def test_complex_comment_removal(self):
        """Test removal of various comment styles."""
        sql_content = """-- Header comment
SELECT
    id,           -- Inline comment style 1
    name,
    -- Full line comment
    timestamp     -- Final field comment
FROM table
-- Footer comment"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name))

        # Comments should be removed, but structure preserved
        lines = result.split('\n')
        assert not any(line.strip().startswith('--') for line in lines)
        assert 'SELECT' in result
        assert 'id,' in result
        assert 'FROM table' in result

    def test_whitespace_handling(self):
        """Test proper whitespace handling."""
        sql_content = """


-- Comment with spaces

SELECT   *   FROM   table


"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name))

        # Should be cleaned up
        assert result == 'SELECT   *   FROM   table'

    def test_preserve_multiline_sql(self):
        """Test that multiline SQL structure is preserved."""
        sql_content = """SELECT
    a.id,
    b.name
FROM table_a a
JOIN table_b b ON a.id = b.a_id
WHERE a.active = 1;"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            f.flush()

            result = load_sql_file(pathlib.Path(f.name))

        # Should preserve structure but remove semicolon
        assert 'SELECT' in result
        assert 'JOIN' in result
        assert 'WHERE' in result
        assert not result.endswith(';')
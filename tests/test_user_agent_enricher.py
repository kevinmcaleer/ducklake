"""Tests for user agent enrichment functionality."""
import pytest
import pandas as pd
from ducklake_core.user_agent_enricher import (
    enrich_user_agents,
    parse_with_heuristics,
    parse_with_library,
    add_empty_user_agent_columns,
    create_empty_user_agent_view_sql,
    USER_AGENT_COLUMNS
)


class TestUserAgentEnrichment:
    """Test user agent parsing and enrichment."""

    def test_user_agent_columns_constant(self):
        """Test that USER_AGENT_COLUMNS contains expected fields."""
        expected_columns = [
            'agent_type', 'os', 'os_version', 'browser', 'browser_version',
            'device', 'is_mobile', 'is_tablet', 'is_pc', 'is_bot'
        ]
        assert USER_AGENT_COLUMNS == expected_columns

    def test_enrich_user_agents_empty_dataframe(self):
        """Test enrichment with empty dataframe."""
        df = pd.DataFrame()
        result = enrich_user_agents(df)

        # Should add all user agent columns
        for col in USER_AGENT_COLUMNS:
            assert col in result.columns

    def test_enrich_user_agents_no_user_agent_column(self):
        """Test enrichment when user_agent column is missing."""
        df = pd.DataFrame({'other_col': ['value1', 'value2']})
        result = enrich_user_agents(df, 'user_agent')

        # Should add all user agent columns with null values
        for col in USER_AGENT_COLUMNS:
            assert col in result.columns
            assert result[col].isna().all()

    def test_enrich_user_agents_with_data(self):
        """Test enrichment with actual user agent data."""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Googlebot/2.1 (+http://www.google.com/bot.html)',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15'
        ]

        df = pd.DataFrame({'user_agent': user_agents})
        result = enrich_user_agents(df)

        # Check that all columns are added
        for col in USER_AGENT_COLUMNS:
            assert col in result.columns

        # Check that we have 3 rows
        assert len(result) == 3

        # Check that boolean columns are integers (0 or 1)
        for col in ['is_mobile', 'is_tablet', 'is_pc', 'is_bot']:
            assert result[col].dtype == 'int64'
            assert result[col].isin([0, 1]).all()

    def test_heuristic_parsing_windows_chrome(self):
        """Test heuristic parsing for Windows Chrome."""
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0'
        result = parse_with_heuristics(pd.DataFrame({'user_agent': [ua]}))

        assert result['agent_type'].iloc[0] == 'human'
        assert result['os'].iloc[0] == 'windows'
        assert result['browser'].iloc[0] == 'chrome'
        assert result['device'].iloc[0] == 'pc'
        assert result['is_pc'].iloc[0] == 1
        assert result['is_bot'].iloc[0] == 0

    def test_heuristic_parsing_bot(self):
        """Test heuristic parsing for bots."""
        ua = 'Googlebot/2.1 (+http://www.google.com/bot.html)'
        result = parse_with_heuristics(pd.DataFrame({'user_agent': [ua]}))

        assert result['agent_type'].iloc[0] == 'bot'
        assert result['is_bot'].iloc[0] == 1

    def test_heuristic_parsing_mobile(self):
        """Test heuristic parsing for mobile devices."""
        ua = 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) mobile Safari/604.1'
        result = parse_with_heuristics(pd.DataFrame({'user_agent': [ua]}))

        assert result['device'].iloc[0] == 'mobile'
        assert result['is_mobile'].iloc[0] == 1

    def test_heuristic_parsing_empty_ua(self):
        """Test heuristic parsing with empty user agent."""
        result = parse_with_heuristics(pd.DataFrame({'user_agent': ['']}))

        assert result['agent_type'].iloc[0] == 'unknown'
        assert result['os'].iloc[0] == 'unknown'
        assert result['browser'].iloc[0] == 'unknown'
        assert result['device'].iloc[0] == 'unknown'

    def test_add_empty_user_agent_columns(self):
        """Test adding empty user agent columns to dataframe."""
        df = pd.DataFrame({'existing_col': [1, 2, 3]})
        result = add_empty_user_agent_columns(df)

        # Original column should still exist
        assert 'existing_col' in result.columns

        # All user agent columns should be added
        for col in USER_AGENT_COLUMNS:
            assert col in result.columns
            assert result[col].isna().all()

    def test_create_empty_user_agent_view_sql(self):
        """Test SQL generation for empty user agent view."""
        sql = create_empty_user_agent_view_sql()

        # Should be a string containing SQL
        assert isinstance(sql, str)
        assert 'CREATE OR REPLACE VIEW silver_page_count' in sql
        assert 'lake_page_count' in sql

        # Should contain all user agent columns
        for col in USER_AGENT_COLUMNS:
            assert col in sql


class TestUserAgentParsingFallback:
    """Test user agent parsing with and without the user_agents library."""

    def test_parse_with_library_fallback(self):
        """Test that library parsing falls back gracefully when library is missing."""
        # This test ensures the import error handling works
        df = pd.DataFrame({'user_agent': ['Mozilla/5.0 (Windows NT 10.0; Win64; x64)']})

        # Should not raise an exception even if user_agents library is missing
        try:
            result = parse_with_library(df, 'user_agent')
            # If library is available, check result structure
            for col in USER_AGENT_COLUMNS:
                assert col in result.columns
        except ImportError:
            # This is expected when library is not available
            pass

    def test_enrich_user_agents_handles_import_error(self):
        """Test that enrich_user_agents handles ImportError gracefully."""
        df = pd.DataFrame({'user_agent': ['Mozilla/5.0 (Windows NT 10.0; Win64; x64)']})

        # Should always work, either with library or fallback
        result = enrich_user_agents(df)

        for col in USER_AGENT_COLUMNS:
            assert col in result.columns

        # Should have enriched data
        assert result['agent_type'].iloc[0] in ['human', 'bot']
        assert result['os'].iloc[0] != ''
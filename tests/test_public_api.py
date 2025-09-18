from ducklake_core import __all__ as exported

def test_public_api_minimal():
    expected = { 'simple_refresh', 'run_simple_reports', 'validate_simple_pipeline', 'ensure_core_tables', 'detect_anomalies' }
    assert set(exported) == expected

import duckdb
import pathlib
from ducklake_core.simple_pipeline import simple_refresh

DB_PATH = 'contentlake.ducklake'

EXPECTED_COLS = {
    'url','ip','user_agent','timestamp','dt','agent_type','os','os_version','browser','browser_version','device','is_mobile','is_tablet','is_pc','is_bot'
}

def test_silver_page_count_enrichment():
    conn = duckdb.connect(DB_PATH)
    res = simple_refresh(conn)
    cols = [r[0] for r in conn.execute('DESCRIBE silver_page_count').fetchall()]
    missing = EXPECTED_COLS - set(cols)
    assert not missing, f"Missing expected columns: {missing}"
    sample = conn.execute('SELECT agent_type, os, device FROM silver_page_count LIMIT 5').fetchall()
    assert len(sample) <= 5
    assert 'latest_page_dt' in res and res['latest_page_dt'] is not None

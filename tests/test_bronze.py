import os
import tempfile
import pathlib

import duckdb

from ducklake_core.bronze import ingest_bronze, backfill_manifest_from_bronze


def setup_temp(conn, root):
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS manifest (
      source TEXT,
      run_ts TIMESTAMP,
      dt DATE,
      path TEXT,
      bytes BIGINT,
      content_sha256 TEXT,
      rows BIGINT,
      format TEXT,
      meta JSON
    )
    """
    )


def test_ingest_and_backfill_jsonl(tmp_path):
    db = duckdb.connect(str(tmp_path / "test.duckdb"))
    setup_temp(db, tmp_path)
    lake = tmp_path

    # Create a tiny jsonl file
    raw = tmp_path / "data.jsonl"
    raw.write_text('{"a":1}\n{"a":2}\n', encoding="utf-8")

    dest, status = ingest_bronze(db, lake, "unit", str(raw), "json")
    assert status in ("INGESTED", "SKIPPED_DUPLICATE")

    rows = db.execute("SELECT COUNT(*) FROM manifest WHERE source='unit'").fetchone()[0]
    assert rows == 1

    # Simulate a file placed directly in bronze without manifest row
    bronze_dir = lake / "bronze" / "source=unit" / "format=json" / "dt=2025-01-01" / "run=TEST"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    orphan = bronze_dir / "orphan.jsonl"
    orphan.write_text('{"a":3}\n', encoding="utf-8")

    backfill_manifest_from_bronze(db, lake, "unit")
    rows2 = db.execute("SELECT COUNT(*) FROM manifest WHERE source='unit'").fetchone()[0]
    assert rows2 >= 2


def test_ingest_csv(tmp_path):
    db = duckdb.connect(str(tmp_path / "test.duckdb"))
    setup_temp(db, tmp_path)
    lake = tmp_path

    csv_path = tmp_path / "t.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")

    dest, status = ingest_bronze(db, lake, "csvsrc", str(csv_path), "csv")
    assert pathlib.Path(dest).exists()
    # rows counted from CSV
    r = db.execute("SELECT rows FROM manifest WHERE source='csvsrc'").fetchone()[0]
    assert r == 2

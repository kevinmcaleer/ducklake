"""
Bronze ingestion utilities for DuckLake.

These functions are parameterized to avoid global state:
- conn: duckdb.DuckDBPyConnection
- lake_root: pathlib.Path pointing to the project root (where bronze/silver/gold live)
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import time
from datetime import date, datetime
from typing import Dict, Optional

import duckdb
from .utils import _sql_str, read_sql_for

try:
	import yaml  # type: ignore
except ImportError:  # pragma: no cover
	yaml = None


def sha256_file(p: pathlib.Path) -> str:
	h = hashlib.sha256()
	with p.open("rb") as f:
		for chunk in iter(lambda: f.read(1024 * 1024), b""):
			h.update(chunk)
	return h.hexdigest()


# read_sql_for is provided by utils


def ingest_bronze(
	conn: duckdb.DuckDBPyConnection,
	lake_root: pathlib.Path,
	source_name: str,
	raw_path: str,
	fmt: str,
	dt_override: Optional[str] = None,
	meta: Optional[Dict] = None,
):
	"""Ingest a file into bronze and record a manifest row.

	Returns (dest_path, status) where status is 'INGESTED' or 'SKIPPED_DUPLICATE'.
	"""
	p = pathlib.Path(raw_path)
	if not p.exists():
		raise FileNotFoundError(raw_path)
	dt_ = dt_override or date.today().isoformat()
	try:
		dt_ = datetime.fromisoformat(dt_).date().isoformat()
	except (TypeError, ValueError):
		pass
	run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
	dest_dir = lake_root / "bronze" / f"source={source_name}" / f"format={fmt.lower()}" / f"dt={dt_}" / f"run={run}"
	dest_dir.mkdir(parents=True, exist_ok=True)
	dest = dest_dir / p.name

	fmt_l = fmt.lower()
	original_name = p.name
	if fmt_l in ("yaml", "yml"):
		if yaml is None:
			raise RuntimeError("PyYAML is required to ingest YAML. Install with: uv pip install pyyaml")
		data = yaml.safe_load(p.read_text(encoding="utf-8"))
		if isinstance(data, dict):
			if "items" in data and isinstance(data["items"], list):
				records = data["items"]
			else:
				records = [data]
		elif isinstance(data, list):
			records = data
		else:
			records = [{"value": data}]
		json_path = dest.with_suffix(".json")
		with json_path.open("w", encoding="utf-8") as jf:
			for rec in records:
				jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
		orig_copy = dest.with_suffix(".yaml")
		orig_copy.write_bytes(p.read_bytes())
		dest = json_path
		fmt_l = "json"
		fmt = "json"
		meta = {**(meta or {}), "original_format": "yaml", "original_name": original_name}
	else:
		dest.write_bytes(p.read_bytes())

	digest = sha256_file(dest)
	exists = conn.execute("SELECT 1 FROM manifest WHERE content_sha256 = ?", [digest]).fetchone()
	if exists:
		return str(dest), "SKIPPED_DUPLICATE"

	rows = 0
	if fmt_l == "csv":
		res = conn.execute(f"SELECT count(*) FROM read_csv_auto('{_sql_str(str(dest))}')").fetchone()
		rows = res[0] if res else 0
	elif fmt_l == "json":
		res = conn.execute(f"SELECT count(*) FROM read_json_auto('{_sql_str(str(dest))}')").fetchone()
		rows = res[0] if res else 0
	elif fmt_l in ("parquet", "pq"):
		res = conn.execute(f"SELECT count(*) FROM read_parquet('{_sql_str(str(dest))}')").fetchone()
		rows = res[0] if res else 0

	conn.execute(
		"""
	  INSERT INTO manifest VALUES (?, now(), ?, ?, ?, ?, ?, ?, ?)
	""",
		[
			source_name,
			dt_,
			str(dest),
			dest.stat().st_size,
			digest,
			rows,
			fmt,
			json.dumps(meta or {}),
		],
	)
	return str(dest), "INGESTED"


def backfill_manifest_from_bronze(
	conn: duckdb.DuckDBPyConnection,
	lake_root: pathlib.Path,
	source_name: str,
):
	"""Scan bronze directory for a source and insert any files missing from manifest.

	Useful if files were copied manually or ingested by older scripts without manifest.
	"""
	base = lake_root / "bronze" / f"source={source_name}"
	if not base.exists():
		return
	added = 0
	for fmt_dir in base.iterdir():
		if not fmt_dir.is_dir() or not fmt_dir.name.startswith("format="):
			continue
		fmt = fmt_dir.name.split("=", 1)[1]
		for dt_dir in fmt_dir.iterdir():
			if not dt_dir.is_dir() or not dt_dir.name.startswith("dt="):
				continue
			dt = dt_dir.name.split("=", 1)[1]
			# validate dt format loosely
			try:
				_ = datetime.fromisoformat(dt).date()
			except Exception:
				pass
			for run_dir in dt_dir.iterdir():
				if not run_dir.is_dir() or not run_dir.name.startswith("run="):
					continue
				for f in run_dir.iterdir():
					if not f.is_file():
						continue
					path_str = str(f)
					# If already in manifest by exact path, skip
					if conn.execute("SELECT 1 FROM manifest WHERE path = ? LIMIT 1", [path_str]).fetchone():
						continue
					# Compute digest and skip duplicates by content
					try:
						digest = sha256_file(f)
					except Exception:
						continue
					if conn.execute("SELECT 1 FROM manifest WHERE content_sha256 = ? LIMIT 1", [digest]).fetchone():
						continue
					# Count rows via DuckDB reader
					read_sql = read_sql_for(conn, path_str, fmt)
					try:
						r = conn.execute(f"SELECT count(*) FROM ({read_sql})").fetchone()
						nrows = r[0] if r else 0
					except duckdb.Error:
						nrows = 0
					meta = {}
					try:
						conn.execute(
							"""
							INSERT INTO manifest VALUES (?, now(), ?, ?, ?, ?, ?, ?, ?)
							""",
							[
								source_name,
								dt,
								path_str,
								f.stat().st_size,
								digest,
								nrows,
								fmt,
								json.dumps(meta),
							],
						)
						added += 1
					except duckdb.Error:
						# best-effort backfill; continue
						continue
	if added:
		print(f"Backfilled manifest for source={source_name}: {added} new file(s)")


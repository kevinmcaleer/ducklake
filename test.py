import duckdb, os
os.makedirs("data", exist_ok=True)  # where Parquet files will live

con = duckdb.connect()
con.install_extension("ducklake")
con.load_extension("ducklake")

# 1) Attach a DuckLake: metadata DB file + a data path for Parquet
con.execute("""
ATTACH 'ducklake:mylake_metadata.duckdb' AS my_lake (DATA_PATH 'data/');
""")

# 2) Use it and create a schema/table
con.execute("USE my_lake;")
con.execute("CREATE SCHEMA IF NOT EXISTS logs;")
con.execute("""
CREATE TABLE IF NOT EXISTS logs.events(
  ts TIMESTAMP,
  level VARCHAR,
  message VARCHAR,
  user_id VARCHAR
);
""")

# 3) Add a few rows
con.execute("""
INSERT INTO logs.events VALUES
  ('2025-08-31 12:00:01','INFO','User login','alice'),
  ('2025-08-31 12:00:05','ERROR','Database timeout','system'),
  ('2025-08-31 12:01:00','WARN','Slow query','bob');
""")

# 4) Query
print(con.execute("""
SELECT level, COUNT(*) AS n
FROM logs.events
GROUP BY level
ORDER BY n DESC
""").fetchdf())

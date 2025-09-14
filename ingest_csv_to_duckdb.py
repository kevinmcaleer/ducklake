import duckdb
import pathlib
import re

DB_PATH = 'duckdb_utils/lake.duckdb'
CSV_DIR = pathlib.Path('_tmp_downloads')
PATTERN = re.compile(r'all-visits-(\d{4}-\d{2}-\d{2})\.csv$')

con = duckdb.connect(DB_PATH)
con.execute('''
CREATE TABLE IF NOT EXISTS bronze_all_visits AS SELECT * FROM read_csv_auto('_tmp_downloads/all-visits-*.csv') WHERE 0=1;
''')

for csv_file in sorted(CSV_DIR.glob('all-visits-*.csv')):
    m = PATTERN.match(csv_file.name)
    if not m:
        print(f'Skipping {csv_file.name}: does not match pattern')
        continue
    print(f'Ingesting {csv_file.name} ...')
    con.execute(f"""
        INSERT INTO bronze_all_visits SELECT * FROM read_csv_auto('{csv_file.as_posix()}');
    """)
    print(f'✔️ Ingested {csv_file.name}')

con.close()
print('All CSVs ingested into bronze_all_visits table.')

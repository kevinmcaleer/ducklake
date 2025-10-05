import duckdb
conn = duckdb.connect('test_simple.duckdb')
conn.execute('CREATE TABLE test_table (id INTEGER, name VARCHAR)')
conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
print('Created test_simple.duckdb with test_table')
print(conn.execute('SELECT * FROM test_table').fetchall())
conn.close()

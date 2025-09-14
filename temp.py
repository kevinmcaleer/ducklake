import duckdb, pandas as pd

# Load a DataFrame
url = 'https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv'
df = pd.read_csv(url)

# Query a DataFrame directly (DuckDB auto-registers the variable name df)
res = duckdb.query("""
  SELECT day, time, ROUND(AVG(total_bill), 2) AS avg_bill
  FROM df
  GROUP BY day, time
  ORDER BY avg_bill DESC
""").df()

print(res.head())
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from pathlib import Path

# Load data
reports_dir = Path('reports')
daily = pd.read_csv(reports_dir / 'visits_pages_daily.csv', parse_dates=['dt'])
monthly = pd.read_csv(reports_dir / 'visits_pages_monthly.csv')
weekly = pd.read_csv(reports_dir / 'visits_pages_weekly.csv')
wow = pd.read_csv(reports_dir / 'visits_pages_wow.csv')
searches = pd.read_csv(reports_dir / 'searches_daily.csv')

# Prepare data
monthly['yyyymm'] = monthly['yyyymm'].astype(str)
search_top = searches.groupby('query', as_index=False)['cnt'].sum().sort_values('cnt', ascending=False).head(20)

# Create figures
daily_fig = px.line(daily, x='dt', y='cnt', title='Daily Page Visits', labels={'dt': 'Date', 'cnt': 'Visits'})
monthly_fig = px.bar(monthly, x='yyyymm', y='cnt', title='Monthly Page Visits', labels={'yyyymm': 'Year-Month', 'cnt': 'Visits'})
weekly_fig = px.bar(weekly, x='iso_week', y='cnt', title='Weekly Page Visits', labels={'iso_week': 'ISO Week', 'cnt': 'Visits'})
top_search_fig = px.bar(search_top, x='query', y='cnt', title='Top 20 Search Queries', labels={'query': 'Query', 'cnt': 'Count'})

# Dash app
app = Dash(__name__)
app.layout = html.Div([
    html.H1('DuckLake Data Dashboard'),
    html.H2('Daily Page Visits'),
    dcc.Graph(figure=daily_fig),
    html.H2('Monthly Page Visits'),
    dcc.Graph(figure=monthly_fig),
    html.H2('Weekly Page Visits'),
    dcc.Graph(figure=weekly_fig),
    html.H2('Top 20 Search Queries'),
    dcc.Graph(figure=top_search_fig),
])

if __name__ == '__main__':
    app.run(debug=True)

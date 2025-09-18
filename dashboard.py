#!/usr/bin/env python
"""Unified Ducklake Dashboard (composition + trends).

Tabs:
1. Composition (live from DuckDB views):
     - Page visits by OS
     - Page visits by Agent Type (human vs bot)
     - Page visits by Device Type
2. Trends (from generated CSV reports in `reports/`):
     - Daily Page Visits (line)
     - Weekly Page Visits (bar)
     - Monthly Page Visits (bar)
     - Top 20 Search Queries (bar)

Prerequisites:
    Run `python run_refresh.py` first to ensure reports & enriched view exist.

Usage:
    python dashboard.py --db contentlake.ducklake --port 8050
"""
from __future__ import annotations
import argparse, duckdb, pathlib
from typing import Optional
import pandas as pd
from dash import Dash, dcc, html
import plotly.express as px
from pathlib import Path


def load_composition_frames(conn: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:
    # Pull fresh aggregates from the view
    queries = {
        'os': "SELECT COALESCE(NULLIF(os,''),'unknown') AS os, count(*) AS views FROM silver_page_count GROUP BY 1 ORDER BY views DESC",
        'agent_type': "SELECT agent_type, count(*) AS views FROM silver_page_count GROUP BY 1 ORDER BY views DESC",
        'device': "SELECT COALESCE(NULLIF(device,''),'unknown') AS device, count(*) AS views FROM silver_page_count GROUP BY 1 ORDER BY views DESC",
    }
    frames = {}
    for k, sql in queries.items():
        try:
            frames[k] = conn.execute(sql).df()
        except Exception:
            frames[k] = pd.DataFrame(columns=[k, 'views'])
    return frames


def load_trend_frames(reports_dir: Path) -> dict[str, pd.DataFrame]:
    def _safe_csv(name: str, parse_dt: bool = False, dt_col: str = 'dt'):
        p = reports_dir / name
        if not p.exists():
            return pd.DataFrame()
        try:
            if parse_dt:
                return pd.read_csv(p, parse_dates=[dt_col])
            return pd.read_csv(p)
        except Exception:
            return pd.DataFrame()
    frames = {
        'daily': _safe_csv('visits_pages_daily.csv', parse_dt=True),
        'weekly': _safe_csv('visits_pages_weekly.csv'),
        'monthly': _safe_csv('visits_pages_monthly.csv'),
        'wow': _safe_csv('visits_pages_wow.csv'),
        'searches_daily': _safe_csv('searches_daily.csv'),
    }
    return frames


def build_app(conn: duckdb.DuckDBPyConnection, db_path: str) -> Dash:
    composition = load_composition_frames(conn)
    reports_dir = Path('reports')
    trends = load_trend_frames(reports_dir)
    app = Dash(__name__)

    # Composition figs
    comp_figs = {}
    if not composition['os'].empty:
        comp_figs['os'] = px.pie(composition['os'], names='os', values='views', title='Page Visits by OS')
    if not composition['agent_type'].empty:
        comp_figs['agent_type'] = px.pie(composition['agent_type'], names='agent_type', values='views', title='Page Visits by Agent Type')
    if not composition['device'].empty:
        comp_figs['device'] = px.pie(composition['device'], names='device', values='views', title='Page Visits by Device Type')

    # Trend figs
    trend_figs = {}
    daily_df = trends['daily']
    if not daily_df.empty:
        trend_figs['daily'] = px.line(daily_df, x='dt', y='cnt', title='Daily Page Visits', labels={'dt':'Date','cnt':'Visits'})
    weekly_df = trends['weekly']
    if not weekly_df.empty:
        trend_figs['weekly'] = px.bar(weekly_df, x='iso_week', y='cnt', title='Weekly Page Visits', labels={'iso_week':'ISO Week','cnt':'Visits'})
    monthly_df = trends['monthly']
    if not monthly_df.empty:
        trend_figs['monthly'] = px.bar(monthly_df, x='yyyymm', y='cnt', title='Monthly Page Visits', labels={'yyyymm':'Year-Month','cnt':'Visits'})
    searches_df = trends['searches_daily']
    if not searches_df.empty:
        top_search = (searches_df.groupby('query', as_index=False)['cnt']
                      .sum().sort_values('cnt', ascending=False).head(20))
        trend_figs['top_search'] = px.bar(top_search, x='query', y='cnt', title='Top 20 Search Queries', labels={'query':'Query','cnt':'Count'})

    app.layout = html.Div([
        html.H1('Ducklake Dashboard', style={'fontFamily':'sans-serif'}),
        dcc.Tabs([
            dcc.Tab(label='Composition', children=[
                html.Div([
                    dcc.Graph(figure=comp_figs.get('os')),
                    dcc.Graph(figure=comp_figs.get('agent_type')),
                    dcc.Graph(figure=comp_figs.get('device')),
                ], style={'display':'grid','gridTemplateColumns':'repeat(auto-fit, minmax(340px, 1fr))','gap':'1rem'}),
            ]),
            dcc.Tab(label='Trends', children=[
                html.Div([
                    dcc.Graph(figure=trend_figs.get('daily')),
                    dcc.Graph(figure=trend_figs.get('weekly')),
                    dcc.Graph(figure=trend_figs.get('monthly')),
                    dcc.Graph(figure=trend_figs.get('top_search')),
                ])
            ]),
        ]),
        html.Div(
            f'DB: {db_path} — refresh data with run_refresh.py then reload browser.',
            style={'marginTop':'0.75rem','fontSize':'0.85em','color':'#555'}
        )
    ])
    return app


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description='Ducklake dashboard (pie charts)')
    ap.add_argument('--db', default='contentlake.ducklake', help='DuckDB database path')
    ap.add_argument('--host', default='127.0.0.1', help='Host to bind')
    ap.add_argument('--port', type=int, default=8050, help='Port to bind')
    args = ap.parse_args(argv)
    conn = duckdb.connect(args.db, read_only=True)
    app = build_app(conn, args.db)
    app.run(host=args.host, port=args.port, debug=True)
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())

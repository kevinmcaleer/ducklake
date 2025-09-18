#!/usr/bin/env python
"""Minimal Dash dashboard for Ducklake page visit composition.

Charts:
- Page visits by Operating System
- Page visits by Agent Type (human vs bot)
- Page visits by Device Type

Data Source:
Relies on the enriched view `silver_page_count` produced by `simple_refresh`.
Run `python run_refresh.py` before launching for fresh data.

Usage:
  python dashboard.py --db contentlake.ducklake --port 8050
"""
from __future__ import annotations
import argparse, duckdb, pathlib
from typing import Optional
import pandas as pd
from dash import Dash, dcc, html
import plotly.express as px


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


def build_app(conn: duckdb.DuckDBPyConnection) -> Dash:
    frames = load_composition_frames(conn)
    app = Dash(__name__)

    # Build figures (guard empty frames)
    figs = {}
    if not frames['os'].empty:
        figs['os'] = px.pie(frames['os'], names='os', values='views', title='Page Visits by OS')
    if not frames['agent_type'].empty:
        figs['agent_type'] = px.pie(frames['agent_type'], names='agent_type', values='views', title='Page Visits by Agent Type')
    if not frames['device'].empty:
        figs['device'] = px.pie(frames['device'], names='device', values='views', title='Page Visits by Device Type')

    app.layout = html.Div([
        html.H1('Ducklake Page Visit Composition', style={'fontFamily': 'sans-serif'}),
        html.Div([
            dcc.Graph(figure=figs.get('os')),
            dcc.Graph(figure=figs.get('agent_type')),
            dcc.Graph(figure=figs.get('device')),
        ], style={'display': 'grid', 'gridTemplateColumns': 'repeat(auto-fit, minmax(340px, 1fr))', 'gap': '1rem'}),
        html.Div(id='footer', children='Refresh data via simple_refresh then reload this page.', style={'marginTop': '1rem', 'fontSize': '0.9em', 'color': '#555'}),
    ])
    return app


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description='Ducklake dashboard (pie charts)')
    ap.add_argument('--db', default='contentlake.ducklake', help='DuckDB database path')
    ap.add_argument('--host', default='127.0.0.1', help='Host to bind')
    ap.add_argument('--port', type=int, default=8050, help='Port to bind')
    args = ap.parse_args(argv)
    conn = duckdb.connect(args.db, read_only=True)
    app = build_app(conn)
    app.run(host=args.host, port=args.port, debug=True)
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())

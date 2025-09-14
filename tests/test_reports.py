import csv
import os
from datetime import date, timedelta
import pytest

REPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'reports')

def read_csv_rows(filename):
    with open(os.path.join(REPORTS_DIR, filename), newline='') as f:
        return list(csv.DictReader(f))

def test_busiest_days_of_week():
    rows = read_csv_rows('busiest_days_of_week.csv')
    # Should have 7 rows, one for each day of week (0=Sun..6=Sat)
    assert len(rows) == 7
    expected_dows = {'Sun','Mon','Tue','Wed','Thu','Fri','Sat'}
    assert set(r['dow'] for r in rows) == expected_dows
    assert set(r['dow_num'] for r in rows) == {str(i) for i in range(7)}
    assert all(int(r['visits']) > 0 for r in rows)
    assert all(int(r['searches']) > 0 for r in rows)

    # Check that sum of visits/searches matches daily totals
    total_visits = sum(int(r['visits']) for r in rows)
    total_searches = sum(int(r['searches']) for r in rows)
    visits_daily = sum(int(r['cnt']) for r in read_csv_rows('visits_pages_daily.csv'))
    # For searches, sum all cnts in searches_daily.csv
    searches_daily = sum(int(r['cnt']) for r in read_csv_rows('searches_daily.csv') if r.get('cnt'))
    assert total_visits == visits_daily, f"Sum of visits by day of week ({total_visits}) != total visits ({visits_daily})"
    assert total_searches == searches_daily, f"Sum of searches by day of week ({total_searches}) != total searches ({searches_daily})"

def test_busiest_hours_utc():
    rows = read_csv_rows('busiest_hours_utc.csv')
    # Should have 24 rows, one for each hour
    assert len(rows) == 24
    expected_hours = {f'{i:02}' for i in range(24)}
    assert set(r['hour_utc'] for r in rows) == expected_hours
    assert all(int(r['visits']) >= 0 for r in rows)
    assert all(int(r['searches']) >= 0 for r in rows)

    # Check that sum of visits/searches matches daily totals
    total_visits = sum(int(r['visits']) for r in rows)
    total_searches = sum(int(r['searches']) for r in rows)
    visits_daily = sum(int(r['cnt']) for r in read_csv_rows('visits_pages_daily.csv'))
    searches_daily = sum(int(r['cnt']) for r in read_csv_rows('searches_daily.csv') if r.get('cnt'))
    assert total_visits == visits_daily, f"Sum of visits by hour ({total_visits}) != total visits ({visits_daily})"
    assert total_searches == searches_daily, f"Sum of searches by hour ({total_searches}) != total searches ({searches_daily})"

def test_engagement_reports():
    for fname in ['engagement_least.csv', 'engagement_most.csv']:
        rows = read_csv_rows(fname)
        # Should have url, views, uniq_visitors columns
        assert all('url' in r and 'views' in r and 'uniq_visitors' in r for r in rows)
        assert all(int(r['views']) >= 0 for r in rows if r['views'])
        assert all(int(r['uniq_visitors']) >= 0 for r in rows if r['uniq_visitors'])

def test_searches_daily_includes_recent():
    rows = read_csv_rows('searches_daily.csv')
    today = date.today()
    yesterday = today - timedelta(days=1)
    dt_set = {r['dt'] for r in rows}
    # Should include at least yesterday or today
    assert str(today) in dt_set or str(yesterday) in dt_set

def test_searches_today_not_empty():
    rows = read_csv_rows('searches_today.csv')
    # Should have at least one row with non-empty query
    assert any(r.get('query') for r in rows)

def test_searches_today_totals_not_empty():
    rows = read_csv_rows('searches_today_totals.csv')
    # Should have at least one row with a count > 0
    assert any(int(r.get('searches_today', 0)) > 0 for r in rows)

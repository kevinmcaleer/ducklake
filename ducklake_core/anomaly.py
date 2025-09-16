import duckdb, json, statistics, math, pathlib, datetime as _dt
from typing import Dict, List

ROOT = pathlib.Path(__file__).resolve().parent.parent
REPORTS_DIR = ROOT / 'reports'

# Simple robust anomaly detection using Median Absolute Deviation (MAD)
# For each metric series (dt, value) compute modified z-score.

def _mad(vals: List[float]) -> float:
    if not vals:
        return 0.0
    med = statistics.median(vals)
    abs_dev = [abs(v - med) for v in vals]
    mad = statistics.median(abs_dev)
    return mad or 0.0

def modified_z_scores(vals: List[float]) -> List[float]:
    if not vals:
        return []
    med = statistics.median(vals)
    mad = _mad(vals)
    if mad == 0:
        # Fallback to standard deviation
        if len(vals) < 2:
            return [0.0] * len(vals)
        stdev = statistics.pstdev(vals) or 1.0
        return [(v - med) / stdev for v in vals]
    return [0.6745 * (v - med) / mad for v in vals]

def detect_anomalies(conn: duckdb.DuckDBPyConnection, z_threshold: float = 6.0) -> Dict:
    """Detect anomalies in daily aggregates (page_views_daily.views) and searches_daily total searches.

    Returns JSON-serializable dict written to reports/anomalies.json.
    """
    out = { 'generated_at': _dt.datetime.utcnow().isoformat()+'Z', 'series': {} }
    # Page views series
    try:
        rows = conn.execute("SELECT dt, views FROM page_views_daily ORDER BY dt").fetchall()
        dts = [r[0] for r in rows]
        vals = [r[1] for r in rows]
        z = modified_z_scores(vals)
        anomalies = []
        for dtv, v, zscore in zip(dts, vals, z):
            if abs(zscore) >= z_threshold:
                anomalies.append({'dt': str(dtv), 'value': v, 'z': round(zscore,3)})
        out['series']['page_views_daily'] = { 'points': len(vals), 'anomalies': anomalies }
    except Exception as e:
        out['series']['page_views_daily_error'] = str(e)

    # Searches total per day
    try:
        rows = conn.execute("SELECT dt, sum(cnt) AS searches FROM searches_daily GROUP BY 1 ORDER BY 1").fetchall()
        dts = [r[0] for r in rows]
        vals = [r[1] for r in rows]
        z = modified_z_scores(vals)
        anomalies = []
        for dtv, v, zscore in zip(dts, vals, z):
            if abs(zscore) >= z_threshold:
                anomalies.append({'dt': str(dtv), 'value': v, 'z': round(zscore,3)})
        out['series']['searches_daily'] = { 'points': len(vals), 'anomalies': anomalies }
    except Exception as e:
        out['series']['searches_daily_error'] = str(e)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / 'anomalies.json').write_text(json.dumps(out, indent=2))
    return out

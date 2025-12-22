"""Generate public dashboard data from DuckDB for kevsrobots.com stats page.

This module generates aggregated, anonymized statistics suitable for public display.
NO PERSONALLY IDENTIFIABLE INFORMATION (PII) should be included in the output.
"""
import duckdb
from datetime import datetime, date, timedelta
from typing import Any
import re
from .ip_geolocation import IPGeolocator, get_country_name


def extract_page_type(url: str) -> str:
    """Extract page type from URL path.

    Examples:
        /blog/post-name -> blog
        /learn/course/lesson -> course
        /resources/projects/project-name -> project
    """
    if not url:
        return 'other'

    # Parse path from URL
    if '://' in url:
        path = url.split('://', 1)[1]
        if '/' in path:
            path = '/' + path.split('/', 1)[1]
        else:
            return 'home'
    else:
        path = url

    # Extract first path segment
    parts = [p for p in path.split('/') if p]
    if not parts:
        return 'home'

    first_segment = parts[0].lower()

    # Map to page types
    if first_segment == 'blog':
        return 'blog'
    elif first_segment == 'learn':
        return 'course'
    elif first_segment in ('resources', 'project', 'projects'):
        return 'project'
    elif first_segment in ('review', 'reviews'):
        return 'review'
    elif first_segment in ('about', 'contact'):
        return 'info'
    else:
        return 'other'


def parse_user_agent(user_agent: str) -> dict[str, str]:
    """Parse user agent string to extract device type and OS.

    Returns dict with 'device' and 'os' keys.
    """
    if not user_agent:
        return {'device': 'unknown', 'os': 'unknown'}

    ua_lower = user_agent.lower()

    # Detect device type
    if 'mobile' in ua_lower or 'android' in ua_lower or 'iphone' in ua_lower:
        device = 'mobile'
    elif 'tablet' in ua_lower or 'ipad' in ua_lower:
        device = 'tablet'
    elif 'bot' in ua_lower or 'crawler' in ua_lower or 'spider' in ua_lower:
        device = 'bot'
    else:
        device = 'desktop'

    # Detect OS
    if 'windows' in ua_lower:
        os_name = 'Windows'
    elif 'mac os' in ua_lower or 'macos' in ua_lower or 'macintosh' in ua_lower:
        os_name = 'macOS'
    elif 'linux' in ua_lower:
        os_name = 'Linux'
    elif 'android' in ua_lower:
        os_name = 'Android'
    elif 'ios' in ua_lower or 'iphone' in ua_lower or 'ipad' in ua_lower:
        os_name = 'iOS'
    else:
        os_name = 'Other'

    return {'device': device, 'os': os_name}


def generate_dashboard_data(db_path: str = 'contentlake.ducklake') -> dict[str, Any]:
    """Generate all dashboard metrics as JSON.

    Args:
        db_path: Path to DuckDB database file

    Returns:
        Dictionary containing all dashboard data with no PII
    """
    con = duckdb.connect(db_path, read_only=True)

    # Calculate date ranges
    today = date.today()
    year_ago = today - timedelta(days=365)
    month_ago = today - timedelta(days=30)
    week_ago = today - timedelta(days=7)

    try:
        # 1. Page visits per day over last year
        visits_per_day = con.execute("""
            SELECT
                dt::VARCHAR as date,
                COUNT(*) as count
            FROM lake_page_count
            WHERE dt >= ?
            GROUP BY dt
            ORDER BY dt
        """, [year_ago]).fetchall()

        # 2. Searches per day over last year
        searches_per_day = con.execute("""
            SELECT
                dt::VARCHAR as date,
                SUM(cnt) as count
            FROM searches_daily
            WHERE dt >= ?
            GROUP BY dt
            ORDER BY dt
        """, [year_ago]).fetchall()

        # 3. Top searches this week
        top_searches_week = con.execute("""
            SELECT query, SUM(cnt) as count
            FROM searches_daily
            WHERE dt >= ?
            GROUP BY query
            ORDER BY count DESC
            LIMIT 5
        """, [week_ago]).fetchall()

        # 3. Top searches this month
        top_searches_month = con.execute("""
            SELECT query, SUM(cnt) as count
            FROM searches_daily
            WHERE dt >= ?
            GROUP BY query
            ORDER BY count DESC
            LIMIT 10
        """, [month_ago]).fetchall()

        # 4. Search trends (movers) - compare this week to last week
        trends = con.execute("""
            WITH this_week AS (
                SELECT query, SUM(cnt) as count,
                       ROW_NUMBER() OVER (ORDER BY SUM(cnt) DESC) as rank
                FROM searches_daily
                WHERE dt >= ?
                GROUP BY query
            ),
            last_week AS (
                SELECT query, SUM(cnt) as count,
                       ROW_NUMBER() OVER (ORDER BY SUM(cnt) DESC) as rank
                FROM searches_daily
                WHERE dt >= ? AND dt < ?
                GROUP BY query
            ),
            historical AS (
                SELECT DISTINCT query
                FROM searches_daily
                WHERE dt < ?
            )
            SELECT
                tw.query,
                tw.count as this_week_count,
                tw.rank as this_week_rank,
                COALESCE(lw.rank, 999) as last_week_rank,
                COALESCE(lw.rank, 999) - tw.rank as rank_change,
                CASE WHEN h.query IS NULL THEN TRUE ELSE FALSE END as is_new
            FROM this_week tw
            LEFT JOIN last_week lw ON tw.query = lw.query
            LEFT JOIN historical h ON tw.query = h.query
            WHERE tw.rank <= 10
            ORDER BY tw.rank
        """, [week_ago, week_ago - timedelta(days=7), week_ago, week_ago]).fetchall()

        # 5. Device and OS breakdown (from page_count user_agent)
        # Parse user agents on-the-fly
        user_agents = con.execute("""
            SELECT DISTINCT user_agent
            FROM lake_page_count
            WHERE dt >= ?
            LIMIT 10000
        """, [month_ago]).fetchall()

        device_counts = {'desktop': 0, 'mobile': 0, 'tablet': 0, 'bot': 0}
        os_counts = {}

        for (ua,) in user_agents:
            parsed = parse_user_agent(ua)
            device_counts[parsed['device']] = device_counts.get(parsed['device'], 0) + 1
            os_counts[parsed['os']] = os_counts.get(parsed['os'], 0) + 1

        # 6. Most popular pages (last 30 days)
        popular_pages = con.execute("""
            SELECT url, COUNT(*) as visits
            FROM lake_page_count
            WHERE dt >= ?
            GROUP BY url
            ORDER BY visits DESC
            LIMIT 20
        """, [month_ago]).fetchall()

        # 7. Page type breakdown
        page_type_counts = {}
        for url, visits in popular_pages:
            page_type = extract_page_type(url)
            page_type_counts[page_type] = page_type_counts.get(page_type, 0) + visits

        # 8. User statistics (based on unique IPs - anonymized)
        # New users = IPs seen only in last 30 days, not before
        # Returning users = IPs seen in last 30 days AND before
        user_stats = con.execute("""
            WITH recent_ips AS (
                SELECT DISTINCT ip
                FROM lake_page_count
                WHERE dt >= ?
            ),
            older_ips AS (
                SELECT DISTINCT ip
                FROM lake_page_count
                WHERE dt < ? AND dt >= ?
            )
            SELECT
                COUNT(DISTINCT r.ip) as total_recent,
                COUNT(DISTINCT o.ip) as returning_count
            FROM recent_ips r
            LEFT JOIN older_ips o ON r.ip = o.ip
        """, [month_ago, month_ago, month_ago - timedelta(days=60)]).fetchone()

        total_users = user_stats[0] if user_stats else 0
        returning_users = user_stats[1] if user_stats else 0
        new_users = total_users - returning_users

        # 9. Total visits and searches
        total_visits = con.execute("""
            SELECT COUNT(*) FROM lake_page_count WHERE dt >= ?
        """, [month_ago]).fetchone()[0]

        total_searches = con.execute("""
            SELECT SUM(cnt) FROM searches_daily WHERE dt >= ?
        """, [month_ago]).fetchone()[0] or 0

        # 10. Country breakdown (from IP geolocation)
        country_counts = {}
        try:
            with IPGeolocator() as geolocator:
                # Get unique IPs from last month with visit counts
                ip_visits = con.execute("""
                    SELECT ip, COUNT(*) as visits
                    FROM lake_page_count
                    WHERE dt >= ? AND ip IS NOT NULL AND ip != ''
                    GROUP BY ip
                """, [month_ago]).fetchall()

                print(f"Looking up countries for {len(ip_visits):,} unique IPs...")

                # Lookup countries for each IP
                for ip, visits in ip_visits:
                    country_code = geolocator.lookup(ip)
                    if country_code:
                        country_name = get_country_name(country_code)
                        country_counts[country_name] = country_counts.get(country_name, 0) + visits
                    else:
                        country_counts['Unknown'] = country_counts.get('Unknown', 0) + visits

                print(f"Found {len(country_counts)} countries")

        except Exception as e:
            print(f"Warning: Could not generate country breakdown: {e}")
            country_counts = {'Unknown': total_visits}

        # Sort countries by visit count
        country_breakdown = sorted(
            [{"country": country, "visits": count} for country, count in country_counts.items()],
            key=lambda x: x['visits'],
            reverse=True
        )[:20]  # Top 20 countries

        # =====================================================================
        # VIRAL DETECTION REPORTS
        # =====================================================================

        # 11. Traffic Anomaly Detector - pages with unusual spikes
        yesterday = today - timedelta(days=1)
        two_days_ago = today - timedelta(days=2)
        traffic_anomalies = con.execute("""
            WITH recent_traffic AS (
                SELECT url, COUNT(*) as recent_visits, COUNT(DISTINCT ip) as recent_unique
                FROM lake_page_count
                WHERE dt >= ? AND ip NOT LIKE '66.249.%'
                GROUP BY url
            ),
            baseline_traffic AS (
                SELECT url, COUNT(*) / 7.0 as avg_daily_visits
                FROM lake_page_count
                WHERE dt >= ? AND dt < ? AND ip NOT LIKE '66.249.%'
                GROUP BY url
            )
            SELECT
                r.url,
                r.recent_visits,
                r.recent_unique,
                COALESCE(b.avg_daily_visits, 0) as baseline,
                CASE
                    WHEN COALESCE(b.avg_daily_visits, 0) > 0
                    THEN ROUND((r.recent_visits - b.avg_daily_visits * 2) / b.avg_daily_visits * 100, 0)
                    ELSE 9999
                END as pct_increase
            FROM recent_traffic r
            LEFT JOIN baseline_traffic b ON r.url = b.url
            WHERE r.recent_visits > 20
              AND (b.avg_daily_visits IS NULL OR r.recent_visits > b.avg_daily_visits * 4)
            ORDER BY r.recent_visits DESC
            LIMIT 10
        """, [two_days_ago, week_ago - timedelta(days=7), two_days_ago]).fetchall()

        # 12. Trending Pages (Week-over-Week)
        trending_pages = con.execute("""
            WITH this_week AS (
                SELECT url, COUNT(*) as visits, COUNT(DISTINCT ip) as unique_visitors,
                       ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
                FROM lake_page_count
                WHERE dt >= ? AND ip NOT LIKE '66.249.%'
                GROUP BY url
            ),
            last_week AS (
                SELECT url, COUNT(*) as visits,
                       ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
                FROM lake_page_count
                WHERE dt >= ? AND dt < ? AND ip NOT LIKE '66.249.%'
                GROUP BY url
            ),
            historical AS (
                SELECT DISTINCT url
                FROM lake_page_count
                WHERE dt < ?
            )
            SELECT
                tw.url,
                tw.visits as this_week_visits,
                tw.unique_visitors,
                tw.rank as this_week_rank,
                COALESCE(lw.visits, 0) as last_week_visits,
                COALESCE(lw.rank, 999) as last_week_rank,
                COALESCE(lw.rank, 999) - tw.rank as rank_change,
                CASE WHEN h.url IS NULL THEN TRUE ELSE FALSE END as is_new
            FROM this_week tw
            LEFT JOIN last_week lw ON tw.url = lw.url
            LEFT JOIN historical h ON tw.url = h.url
            WHERE tw.rank <= 15
            ORDER BY tw.rank
        """, [week_ago, week_ago - timedelta(days=7), week_ago, week_ago]).fetchall()

        # 13. Geographic Traffic Shifts - countries with unusual spikes
        geographic_shifts = []
        try:
            with IPGeolocator() as geolocator:
                # Get country visits for last 2 days vs baseline
                recent_ip_visits = con.execute("""
                    SELECT ip, COUNT(*) as visits
                    FROM lake_page_count
                    WHERE dt >= ? AND ip NOT LIKE '66.249.%'
                    GROUP BY ip
                """, [two_days_ago]).fetchall()

                baseline_ip_visits = con.execute("""
                    SELECT ip, COUNT(*) / 7.0 as avg_visits
                    FROM lake_page_count
                    WHERE dt >= ? AND dt < ? AND ip NOT LIKE '66.249.%'
                    GROUP BY ip
                """, [week_ago - timedelta(days=7), two_days_ago]).fetchall()

                recent_countries = {}
                for ip, visits in recent_ip_visits:
                    country_code = geolocator.lookup(ip)
                    if country_code:
                        country_name = get_country_name(country_code)
                        recent_countries[country_name] = recent_countries.get(country_name, 0) + visits

                baseline_countries = {}
                for ip, avg_visits in baseline_ip_visits:
                    country_code = geolocator.lookup(ip)
                    if country_code:
                        country_name = get_country_name(country_code)
                        baseline_countries[country_name] = baseline_countries.get(country_name, 0) + avg_visits

                for country, recent in recent_countries.items():
                    baseline = baseline_countries.get(country, 0) * 2
                    if recent > 50 and (baseline == 0 or recent > baseline * 2):
                        pct_increase = int((recent - baseline) / baseline * 100) if baseline > 0 else 9999
                        geographic_shifts.append({
                            "country": country,
                            "recent_visits": recent,
                            "baseline_daily": round(baseline / 2, 1),
                            "pct_increase": pct_increase
                        })

                geographic_shifts = sorted(geographic_shifts, key=lambda x: x['recent_visits'], reverse=True)[:10]

        except Exception as e:
            print(f"Warning: Could not generate geographic shifts: {e}")

        # 14. New Visitor Surge - daily new vs returning ratio
        new_visitor_surge = con.execute("""
            WITH daily_visitors AS (
                SELECT dt, ip
                FROM lake_page_count
                WHERE dt >= ? AND ip NOT LIKE '66.249.%'
            ),
            first_seen AS (
                SELECT ip, MIN(dt) as first_visit_dt
                FROM lake_page_count
                WHERE ip NOT LIKE '66.249.%'
                GROUP BY ip
            )
            SELECT
                dv.dt::VARCHAR as date,
                COUNT(DISTINCT dv.ip) as total_visitors,
                COUNT(DISTINCT CASE WHEN fs.first_visit_dt = dv.dt THEN dv.ip END) as new_visitors,
                COUNT(DISTINCT CASE WHEN fs.first_visit_dt < dv.dt THEN dv.ip END) as returning_visitors
            FROM daily_visitors dv
            JOIN first_seen fs ON dv.ip = fs.ip
            GROUP BY dv.dt
            ORDER BY dv.dt
        """, [week_ago - timedelta(days=7)]).fetchall()

        # 15. Hourly Traffic Heatmap - last 7 days
        hourly_heatmap = con.execute("""
            SELECT
                dt::VARCHAR as date,
                EXTRACT(HOUR FROM timestamp) as hour,
                COUNT(*) as visits
            FROM lake_page_count
            WHERE dt >= ? AND ip NOT LIKE '66.249.%'
            GROUP BY dt, EXTRACT(HOUR FROM timestamp)
            ORDER BY dt, hour
        """, [week_ago]).fetchall()

        # 16. Bot vs Human Traffic Ratio
        bot_human_traffic = con.execute("""
            SELECT
                dt::VARCHAR as date,
                COUNT(*) as total_visits,
                COUNT(CASE WHEN ip LIKE '66.249.%' THEN 1 END) as google_bot,
                COUNT(CASE WHEN user_agent LIKE '%bot%' OR user_agent LIKE '%Bot%'
                           OR user_agent LIKE '%crawler%' OR user_agent LIKE '%spider%' THEN 1 END) as other_bots,
                COUNT(CASE WHEN ip NOT LIKE '66.249.%'
                           AND user_agent NOT LIKE '%bot%' AND user_agent NOT LIKE '%Bot%'
                           AND user_agent NOT LIKE '%crawler%' AND user_agent NOT LIKE '%spider%' THEN 1 END) as human
            FROM lake_page_count
            WHERE dt >= ?
            GROUP BY dt
            ORDER BY dt
        """, [week_ago - timedelta(days=7)]).fetchall()

        # Format results
        return {
            "last_updated": datetime.now().astimezone().isoformat(),
            "date_range": {
                "today": today.isoformat(),
                "year_ago": year_ago.isoformat(),
                "month_ago": month_ago.isoformat(),
                "week_ago": week_ago.isoformat()
            },
            "visits_per_day": [
                {"date": date_str, "count": count}
                for date_str, count in visits_per_day
            ],
            "searches_per_day": [
                {"date": date_str, "count": count}
                for date_str, count in searches_per_day
            ],
            "top_searches_week": [
                {"query": query, "count": count}
                for query, count in top_searches_week
            ],
            "top_searches_month": [
                {"query": query, "count": count}
                for query, count in top_searches_month
            ],
            "search_trends": [
                {
                    "query": query,
                    "count": this_week_count,
                    "rank": this_week_rank,
                    "rank_change": rank_change,
                    "trend": "up" if rank_change > 0 else ("down" if rank_change < 0 else "stable"),
                    "is_new": is_new
                }
                for query, this_week_count, this_week_rank, last_week_rank, rank_change, is_new in trends
            ],
            "device_breakdown": device_counts,
            "os_breakdown": os_counts,
            "popular_pages": [
                {
                    "url": url,
                    "visits": visits,
                    "page_type": extract_page_type(url)
                }
                for url, visits in popular_pages
            ],
            "page_type_breakdown": page_type_counts,
            "country_breakdown": country_breakdown,
            "user_stats": {
                "new_users": new_users,
                "returning_users": returning_users,
                "total_users": total_users
            },
            "totals": {
                "visits_last_month": total_visits,
                "searches_last_month": total_searches
            },
            # Viral detection reports
            "traffic_anomalies": [
                {
                    "url": url,
                    "recent_visits": recent_visits,
                    "unique_visitors": recent_unique,
                    "baseline_daily": round(baseline, 1),
                    "pct_increase": int(pct_increase) if pct_increase != 9999 else None,
                    "is_new": pct_increase == 9999
                }
                for url, recent_visits, recent_unique, baseline, pct_increase in traffic_anomalies
            ],
            "trending_pages": [
                {
                    "url": url,
                    "visits": this_week_visits,
                    "unique_visitors": unique_visitors,
                    "rank": this_week_rank,
                    "last_week_visits": last_week_visits,
                    "rank_change": rank_change,
                    "is_new": is_new
                }
                for url, this_week_visits, unique_visitors, this_week_rank, last_week_visits, last_week_rank, rank_change, is_new in trending_pages
            ],
            "geographic_shifts": geographic_shifts,
            "new_visitor_surge": [
                {
                    "date": date_str,
                    "total_visitors": total_visitors,
                    "new_visitors": new_visitors,
                    "returning_visitors": returning_visitors,
                    "new_pct": round(new_visitors / total_visitors * 100, 1) if total_visitors > 0 else 0
                }
                for date_str, total_visitors, new_visitors, returning_visitors in new_visitor_surge
            ],
            "hourly_heatmap": [
                {
                    "date": date_str,
                    "hour": int(hour),
                    "visits": visits
                }
                for date_str, hour, visits in hourly_heatmap
            ],
            "bot_human_traffic": [
                {
                    "date": date_str,
                    "total": total,
                    "google_bot": google_bot,
                    "other_bots": other_bots,
                    "human": human,
                    "human_pct": round(human / total * 100, 1) if total > 0 else 0
                }
                for date_str, total, google_bot, other_bots, human in bot_human_traffic
            ]
        }

    finally:
        con.close()


if __name__ == '__main__':
    """Test the dashboard data generator"""
    import json

    print("Generating dashboard data...")
    data = generate_dashboard_data()

    print(f"\nGenerated at: {data['last_updated']}")
    print(f"Searches per day: {len(data['searches_per_day'])} days")
    print(f"Top search this week: {data['top_searches_week'][0] if data['top_searches_week'] else 'N/A'}")
    print(f"Total users (last month): {data['user_stats']['total_users']}")
    print(f"\nFull JSON:\n")
    print(json.dumps(data, indent=2))

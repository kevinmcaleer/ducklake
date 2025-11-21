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
        # 1. Searches per day over last year
        searches_per_day = con.execute("""
            SELECT
                dt::VARCHAR as date,
                SUM(cnt) as count
            FROM searches_daily
            WHERE dt >= ?
            GROUP BY dt
            ORDER BY dt
        """, [year_ago]).fetchall()

        # 2. Top searches this week
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
            )
            SELECT
                tw.query,
                tw.count as this_week_count,
                tw.rank as this_week_rank,
                COALESCE(lw.rank, 999) as last_week_rank,
                COALESCE(lw.rank, 999) - tw.rank as rank_change
            FROM this_week tw
            LEFT JOIN last_week lw ON tw.query = lw.query
            WHERE tw.rank <= 10
            ORDER BY tw.rank
        """, [week_ago, week_ago - timedelta(days=7), week_ago]).fetchall()

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

        # Format results
        return {
            "last_updated": datetime.now().astimezone().isoformat(),
            "date_range": {
                "today": today.isoformat(),
                "year_ago": year_ago.isoformat(),
                "month_ago": month_ago.isoformat(),
                "week_ago": week_ago.isoformat()
            },
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
                    "is_new": last_week_rank >= 999
                }
                for query, this_week_count, this_week_rank, last_week_rank, rank_change in trends
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
            }
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

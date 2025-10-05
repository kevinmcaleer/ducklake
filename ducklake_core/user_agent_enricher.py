"""User agent parsing and enrichment functionality.

Provides clean separation between library-based parsing and fallback heuristics.
"""
from __future__ import annotations
import pandas as pd

# User agent enrichment column names for consistency
USER_AGENT_COLUMNS = [
    'agent_type', 'os', 'os_version', 'browser', 'browser_version',
    'device', 'is_mobile', 'is_tablet', 'is_pc', 'is_bot'
]


def enrich_user_agents(df: pd.DataFrame, user_agent_column: str = 'user_agent') -> pd.DataFrame:
    """Enrich dataframe with user agent parsing results.

    First attempts library-based parsing, falls back to heuristics if library unavailable.
    """
    if user_agent_column not in df.columns or df.empty:
        return add_empty_user_agent_columns(df)

    try:
        return parse_with_library(df, user_agent_column)
    except ImportError:
        return parse_with_heuristics(df, user_agent_column)


def parse_with_library(df: pd.DataFrame, user_agent_column: str) -> pd.DataFrame:
    """Parse user agents using the user_agents library."""
    import user_agents  # type: ignore

    parsed = df[user_agent_column].fillna('').map(lambda ua: user_agents.parse(ua))

    df['agent_type'] = parsed.map(lambda o: 'bot' if o.is_bot else 'human')
    df['os'] = parsed.map(lambda o: o.os.family or 'unknown')
    df['os_version'] = parsed.map(lambda o: o.os.version_string or '')
    df['browser'] = parsed.map(lambda o: o.browser.family or 'unknown')
    df['browser_version'] = parsed.map(lambda o: o.browser.version_string or '')
    df['device'] = parsed.map(lambda o: o.device.family or 'unknown')
    df['is_mobile'] = parsed.map(lambda o: int(o.is_mobile))
    df['is_tablet'] = parsed.map(lambda o: int(o.is_tablet))
    df['is_pc'] = parsed.map(lambda o: int(o.is_pc))
    df['is_bot'] = parsed.map(lambda o: int(o.is_bot))

    return df


def parse_with_heuristics(df: pd.DataFrame, user_agent_column: str) -> pd.DataFrame:
    """Parse user agents using simple heuristics when library unavailable."""
    parsed = df[user_agent_column].fillna('').map(_heuristic_parse)

    df['agent_type'] = parsed.map(lambda t: t[0])
    df['os'] = parsed.map(lambda t: t[1])
    df['os_version'] = parsed.map(lambda t: t[2])
    df['browser'] = parsed.map(lambda t: t[3])
    df['browser_version'] = parsed.map(lambda t: t[4])
    df['device'] = parsed.map(lambda t: t[5])
    df['is_mobile'] = parsed.map(lambda t: t[6])
    df['is_tablet'] = parsed.map(lambda t: t[7])
    df['is_pc'] = parsed.map(lambda t: t[8])
    df['is_bot'] = parsed.map(lambda t: t[9])

    return df


def _heuristic_parse(ua: str) -> tuple[str, str, str, str, str, str, int, int, int, int]:
    """Simple heuristic parsing of user agent string."""
    if not ua:
        return ('unknown', 'unknown', '', 'unknown', '', 'unknown', 0, 0, 0, 0)

    u = ua.lower()

    # Detect agent type
    agent_type = 'bot' if any(x in u for x in ['bot', 'spider', 'crawl']) else 'human'

    # Detect OS
    if 'windows' in u:
        os_name = 'windows'
    elif 'android' in u:
        os_name = 'android'
    elif 'linux' in u:
        os_name = 'linux'
    elif 'mac os' in u or 'macintosh' in u:
        os_name = 'mac'
    else:
        os_name = 'unknown'

    # Detect browser
    if 'chrome/' in u:
        browser = 'chrome'
    elif 'firefox/' in u:
        browser = 'firefox'
    elif 'safari/' in u:
        browser = 'safari'
    else:
        browser = 'unknown'

    # Detect device type
    if 'mobile' in u:
        device = 'mobile'
    elif 'tablet' in u:
        device = 'tablet'
    elif os_name in ('windows', 'linux', 'mac'):
        device = 'pc'
    else:
        device = 'unknown'

    # Set flags
    is_mobile = 1 if device == 'mobile' else 0
    is_tablet = 1 if device == 'tablet' else 0
    is_pc = 1 if device == 'pc' else 0
    is_bot = 1 if agent_type == 'bot' else 0

    return (agent_type, os_name, '', browser, '', device, is_mobile, is_tablet, is_pc, is_bot)


def add_empty_user_agent_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add empty user agent columns to dataframe for schema consistency."""
    for col in USER_AGENT_COLUMNS:
        if col.startswith('is_'):
            df[col] = None  # Will be cast to INTEGER in SQL
        else:
            df[col] = None  # Will be cast to VARCHAR in SQL
    return df


def create_empty_user_agent_view_sql() -> str:
    """Generate SQL for creating empty user agent view with proper schema."""
    from .sql_loader import load_view_sql
    return load_view_sql('silver_page_count_empty')
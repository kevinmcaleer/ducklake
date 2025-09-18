# Advanced user agent parsing for DuckLake
# Uses the 'user-agents' and 'httpagentparser' libraries for robust detection

import re
from user_agents import parse as ua_parse
import httpagentparser

def parse_user_agent(user_agent: str) -> dict:
    """
    Returns a dict with fields:
      - agent_type: 'bot' or 'human'
      - os: operating system (string)
      - os_version: version (if available)
      - browser: browser family (string)
      - browser_version: version (if available)
      - device: device type (mobile/tablet/pc/bot/other)
      - is_mobile: bool
      - is_tablet: bool
      - is_pc: bool
      - is_bot: bool
      - raw: original user agent string
    """
    if not user_agent or user_agent.strip() == '':
        return {
            'agent_type': 'unknown', 'os': None, 'os_version': None, 'browser': None,
            'browser_version': None, 'device': None, 'is_mobile': False, 'is_tablet': False,
            'is_pc': False, 'is_bot': False, 'raw': user_agent
        }
    ua = ua_parse(user_agent)
    agent_type = 'bot' if ua.is_bot else 'human'
    # Fallback: substring match for bots
    bot_regex = re.compile(r"bot|spider|crawl|slurp|search|baidu|bing|duckduck|yandex|sogou|exabot|facebot|ia_archiver", re.I)
    if not ua.is_bot and bot_regex.search(user_agent):
        agent_type = 'bot'
    # Use httpagentparser for more details
    hap = httpagentparser.detect(user_agent)
    os = hap.get('os', {}).get('name')
    os_version = hap.get('os', {}).get('version')
    browser = hap.get('browser', {}).get('name')
    browser_version = hap.get('browser', {}).get('version')
    device = 'bot' if agent_type == 'bot' else (
        'mobile' if ua.is_mobile else 'tablet' if ua.is_tablet else 'pc' if ua.is_pc else 'other'
    )
    return {
        'agent_type': agent_type,
        'os': os,
        'os_version': os_version,
        'browser': browser,
        'browser_version': browser_version,
        'device': device,
        'is_mobile': ua.is_mobile,
        'is_tablet': ua.is_tablet,
        'is_pc': ua.is_pc,
        'is_bot': agent_type == 'bot',
        'raw': user_agent
    }

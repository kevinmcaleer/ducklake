"""
Config loading utilities for DuckLake.
"""
import yaml
from pathlib import Path
from typing import Dict

def load_sources_config(cfg_path: Path) -> Dict:
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

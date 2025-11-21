"""IP Geolocation with caching for country lookups.

Uses GeoIP2 with MaxMind GeoLite2 database for IP-to-country mapping.
Implements persistent caching to avoid repeated lookups of the same IPs.
"""
import geoip2.database
import geoip2.errors
from pathlib import Path
import json
from typing import Optional
from datetime import datetime


class IPGeolocator:
    """IP to country code mapper with persistent caching."""

    def __init__(self, db_path: Optional[Path] = None, cache_path: Optional[Path] = None):
        """Initialize geolocation with database and cache.

        Args:
            db_path: Path to GeoLite2-Country.mmdb file. If None, looks in common locations.
            cache_path: Path to JSON cache file. Defaults to 'data/ip_country_cache.json'
        """
        self.db_path = db_path or self._find_database()
        self.cache_path = cache_path or Path('data/ip_country_cache.json')
        self.cache: dict[str, str] = {}
        self.reader = None
        self._cache_modified = False

        # Load cache from disk
        self._load_cache()

        # Initialize GeoIP2 reader if database exists
        if self.db_path and self.db_path.exists():
            try:
                self.reader = geoip2.database.Reader(str(self.db_path))
            except Exception as e:
                print(f"Warning: Could not open GeoIP2 database: {e}")
                print(f"Country data will be limited to cached results only")

    def _find_database(self) -> Optional[Path]:
        """Find GeoLite2 database in common locations."""
        search_paths = [
            Path('data/GeoLite2-Country.mmdb'),
            Path('/usr/share/GeoIP/GeoLite2-Country.mmdb'),
            Path('/usr/local/share/GeoIP/GeoLite2-Country.mmdb'),
            Path.home() / '.local/share/GeoIP/GeoLite2-Country.mmdb',
        ]

        for path in search_paths:
            if path.exists():
                return path

        return None

    def _load_cache(self):
        """Load IP-to-country cache from disk."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r') as f:
                    self.cache = json.load(f)
                print(f"Loaded {len(self.cache):,} cached IP-to-country mappings")
            except Exception as e:
                print(f"Warning: Could not load cache: {e}")
                self.cache = {}
        else:
            self.cache = {}

    def save_cache(self):
        """Save IP-to-country cache to disk if modified."""
        if not self._cache_modified:
            return

        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=2)
            print(f"Saved {len(self.cache):,} IP-to-country mappings to cache")
            self._cache_modified = False
        except Exception as e:
            print(f"Warning: Could not save cache: {e}")

    def lookup(self, ip: str) -> Optional[str]:
        """Get country code for IP address.

        Args:
            ip: IP address (IPv4 or IPv6)

        Returns:
            ISO 3166-1 alpha-2 country code (e.g., 'US', 'GB', 'JP') or None if unknown
        """
        if not ip or ip == 'unknown':
            return None

        # Check cache first
        if ip in self.cache:
            return self.cache[ip]

        # If no reader available, return None
        if not self.reader:
            return None

        # Lookup in GeoIP2 database
        try:
            response = self.reader.country(ip)
            country_code = response.country.iso_code

            # Cache the result
            self.cache[ip] = country_code
            self._cache_modified = True

            return country_code
        except geoip2.errors.AddressNotFoundError:
            # IP not in database (e.g., private IPs)
            self.cache[ip] = None
            self._cache_modified = True
            return None
        except Exception as e:
            print(f"Warning: Error looking up IP {ip}: {e}")
            return None

    def lookup_batch(self, ips: list[str], save_interval: int = 1000) -> dict[str, Optional[str]]:
        """Lookup multiple IPs efficiently with periodic cache saves.

        Args:
            ips: List of IP addresses to lookup
            save_interval: Save cache every N lookups

        Returns:
            Dictionary mapping IP to country code
        """
        results = {}
        new_lookups = 0

        for i, ip in enumerate(ips):
            results[ip] = self.lookup(ip)

            # Count new lookups (not from cache)
            if self._cache_modified and new_lookups % save_interval == 0:
                self.save_cache()
                new_lookups = 0

            if ip not in self.cache:
                new_lookups += 1

        # Final save
        if self._cache_modified:
            self.save_cache()

        return results

    def close(self):
        """Close database reader and save cache."""
        if self.reader:
            self.reader.close()
        self.save_cache()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure cache is saved."""
        self.close()


def get_country_name(country_code: Optional[str]) -> str:
    """Get full country name from ISO code.

    Args:
        country_code: ISO 3166-1 alpha-2 code (e.g., 'US')

    Returns:
        Full country name (e.g., 'United States')
    """
    if not country_code:
        return 'Unknown'

    # Common country codes (expand as needed)
    country_names = {
        'US': 'United States',
        'GB': 'United Kingdom',
        'CA': 'Canada',
        'AU': 'Australia',
        'DE': 'Germany',
        'FR': 'France',
        'JP': 'Japan',
        'CN': 'China',
        'IN': 'India',
        'BR': 'Brazil',
        'RU': 'Russia',
        'IT': 'Italy',
        'ES': 'Spain',
        'MX': 'Mexico',
        'NL': 'Netherlands',
        'SE': 'Sweden',
        'NO': 'Norway',
        'DK': 'Denmark',
        'FI': 'Finland',
        'PL': 'Poland',
        'BE': 'Belgium',
        'CH': 'Switzerland',
        'AT': 'Austria',
        'IE': 'Ireland',
        'NZ': 'New Zealand',
        'SG': 'Singapore',
        'KR': 'South Korea',
        'TW': 'Taiwan',
        'HK': 'Hong Kong',
        'MY': 'Malaysia',
        'TH': 'Thailand',
        'ID': 'Indonesia',
        'PH': 'Philippines',
        'VN': 'Vietnam',
        'AR': 'Argentina',
        'CL': 'Chile',
        'CO': 'Colombia',
        'PE': 'Peru',
        'ZA': 'South Africa',
        'EG': 'Egypt',
        'SA': 'Saudi Arabia',
        'AE': 'UAE',
        'IL': 'Israel',
        'TR': 'Turkey',
        'GR': 'Greece',
        'PT': 'Portugal',
        'CZ': 'Czech Republic',
        'RO': 'Romania',
        'HU': 'Hungary',
        'UA': 'Ukraine',
    }

    return country_names.get(country_code, country_code)

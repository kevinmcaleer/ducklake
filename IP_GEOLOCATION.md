# IP Geolocation Setup

This document explains how to set up IP-to-country geolocation for the analytics dashboard.

## Overview

The dashboard uses MaxMind's **GeoLite2 Country** database to convert IP addresses to country codes. The system includes:

- **Persistent caching** - IP lookups are cached to `data/ip_country_cache.json`
- **Batch processing** - Efficient lookup of thousands of IPs
- **Graceful degradation** - Works with cached data if database is unavailable

## Setup Instructions

### Option 1: Download with License Key (Recommended)

1. Register for a free MaxMind account:
   - Visit: https://www.maxmind.com/en/geolite2/signup
   - Fill out the registration form

2. Generate a license key:
   - Log in to your MaxMind account
   - Go to: https://www.maxmind.com/en/accounts/current/license-key
   - Click "Generate New License Key"
   - Choose "No" for GeoIP Update Program (we'll update manually)
   - Save your license key

3. Download the database:
   ```bash
   export MAXMIND_LICENSE_KEY='your_license_key_here'
   python3 download_geoip_db.py --download
   ```

### Option 2: Manual Download

1. Register and log in as above

2. Download the database:
   - Go to: https://www.maxmind.com/en/accounts/current/geoip/downloads
   - Find "GeoLite2 Country" and click "Download GZIP"

3. Extract the database:
   ```bash
   tar -xzf GeoLite2-Country_*.tar.gz
   ```

4. Copy to the correct location:
   ```bash
   mkdir -p data
   cp GeoLite2-Country_*/GeoLite2-Country.mmdb data/
   ```

5. Verify installation:
   ```bash
   python3 download_geoip_db.py
   ```

## How It Works

### Caching Strategy

The system maintains a JSON cache file that maps IPs to country codes:

```json
{
  "192.168.1.1": "US",
  "10.0.0.1": null,
  "203.0.113.45": "GB"
}
```

- **First lookup**: Queries the GeoLite2 database, adds to cache
- **Subsequent lookups**: Returns from cache instantly
- **Unknown IPs**: Cached as `null` to avoid repeated failed lookups
- **Auto-save**: Cache is saved periodically during batch operations

### Usage in Dashboard

The dashboard generation in `dashboard_data.py` now includes:

```python
with IPGeolocator() as geolocator:
    for ip, visits in ip_visits:
        country_code = geolocator.lookup(ip)
        if country_code:
            country_name = get_country_name(country_code)
            country_counts[country_name] += visits
```

### Performance

- **Initial run**: ~10-100 lookups/second (database lookups)
- **Cached runs**: Instant (memory + JSON cache)
- **Typical analytics run**:
  - 1,000 unique IPs → ~1-2 seconds first time
  - Same 1,000 IPs → <0.1 seconds with cache

## Cache Management

### View cache stats:
```python
from ducklake_core.ip_geolocation import IPGeolocator

with IPGeolocator() as geo:
    print(f"Cached IPs: {len(geo.cache):,}")
```

### Clear cache (if needed):
```bash
rm data/ip_country_cache.json
```

### Cache location:
- Default: `data/ip_country_cache.json`
- Customize: `IPGeolocator(cache_path=Path('custom/path.json'))`

## Database Updates

MaxMind releases updates twice monthly (first Tuesday and first Thursday). To update:

```bash
# With license key
export MAXMIND_LICENSE_KEY='your_key_here'
python3 download_geoip_db.py --download

# Or download manually and replace:
mv GeoLite2-Country.mmdb data/GeoLite2-Country.mmdb
```

After updating, the cache remains valid - it will only look up new IPs.

## Troubleshooting

### Database not found
```
Warning: Could not open GeoIP2 database
```
**Solution**: Run `python3 download_geoip_db.py` and follow instructions

### License key error
```
Error downloading database: HTTP Error 401: Unauthorized
```
**Solution**: Check your license key is correct and active

### Private IPs showing as Unknown
This is expected - private IPs (192.168.x.x, 10.x.x.x, 127.x.x.x) aren't in the database.

### Cache file permission errors
```
Warning: Could not save cache: Permission denied
```
**Solution**: Ensure `data/` directory is writable:
```bash
mkdir -p data
chmod 755 data
```

## Privacy & Data Retention

- **No PII stored**: Only IP → country code mappings
- **Anonymized aggregation**: Dashboard shows country totals, not individual IPs
- **Cache is safe to commit**: Contains no personal information
- **Compliance**: Country-level aggregation meets GDPR/privacy requirements

## API Reference

See `ducklake_core/ip_geolocation.py` for full API documentation.

### Quick Examples

```python
from ducklake_core.ip_geolocation import IPGeolocator, get_country_name

# Single lookup
with IPGeolocator() as geo:
    country = geo.lookup('8.8.8.8')
    print(f"Country: {get_country_name(country)}")  # "United States"

# Batch lookup
with IPGeolocator() as geo:
    ips = ['8.8.8.8', '1.1.1.1', '208.67.222.222']
    results = geo.lookup_batch(ips)
    for ip, country in results.items():
        print(f"{ip} → {get_country_name(country)}")
```

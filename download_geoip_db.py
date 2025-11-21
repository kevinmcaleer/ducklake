#!/usr/bin/env python3
"""Download MaxMind GeoLite2 Country database.

The GeoLite2 database is free but requires registration at:
https://www.maxmind.com/en/geolite2/signup

After registration, you can download the database from:
https://www.maxmind.com/en/accounts/current/geoip/downloads

This script provides instructions and checks for the database.
"""
import sys
from pathlib import Path
import tarfile
import urllib.request


def check_database():
    """Check if GeoLite2 database exists."""
    db_path = Path('data/GeoLite2-Country.mmdb')

    if db_path.exists():
        print(f"✓ GeoLite2 database found at: {db_path}")
        size_mb = db_path.stat().st_size / (1024 * 1024)
        print(f"  Size: {size_mb:.1f} MB")
        return True
    else:
        print(f"✗ GeoLite2 database not found at: {db_path}")
        return False


def download_instructions():
    """Print download instructions."""
    print("\n" + "="*70)
    print("GeoLite2 Country Database - Download Instructions")
    print("="*70)
    print()
    print("The GeoLite2 database is free but requires registration.")
    print()
    print("Steps to download:")
    print()
    print("1. Register for a free account at:")
    print("   https://www.maxmind.com/en/geolite2/signup")
    print()
    print("2. Log in and go to:")
    print("   https://www.maxmind.com/en/accounts/current/geoip/downloads")
    print()
    print("3. Download: GeoLite2 Country (GZIP)")
    print()
    print("4. Extract the .tar.gz file")
    print()
    print("5. Copy GeoLite2-Country.mmdb to:")
    print(f"   {Path('data/GeoLite2-Country.mmdb').absolute()}")
    print()
    print("Alternative: Direct download with license key")
    print("--------------------------------------------")
    print("If you have a MaxMind license key, you can download directly:")
    print()
    print("  export MAXMIND_LICENSE_KEY='your_key_here'")
    print("  python3 download_geoip_db.py --download")
    print()
    print("="*70)


def download_with_license_key(license_key: str):
    """Download GeoLite2 database using license key.

    Args:
        license_key: MaxMind license key
    """
    print("Downloading GeoLite2 Country database...")

    # MaxMind permalink URL
    url = f"https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country&license_key={license_key}&suffix=tar.gz"

    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)

    tar_path = data_dir / 'GeoLite2-Country.tar.gz'

    try:
        print(f"Downloading to {tar_path}...")
        urllib.request.urlretrieve(url, tar_path)

        print("Extracting database...")
        with tarfile.open(tar_path) as tar:
            # Find the .mmdb file in the archive
            for member in tar.getmembers():
                if member.name.endswith('.mmdb'):
                    # Extract just the database file
                    member.name = 'GeoLite2-Country.mmdb'
                    tar.extract(member, data_dir)
                    print(f"✓ Database extracted to {data_dir / 'GeoLite2-Country.mmdb'}")
                    break

        # Clean up tar file
        tar_path.unlink()
        print("✓ Download complete!")
        return True

    except Exception as e:
        print(f"✗ Error downloading database: {e}")
        print("\nPlease download manually using the instructions above.")
        return False


def main():
    """Main entry point."""
    import argparse
    import os

    parser = argparse.ArgumentParser(description='Download GeoLite2 Country database')
    parser.add_argument('--download', action='store_true',
                       help='Download using MAXMIND_LICENSE_KEY environment variable')
    parser.add_argument('--license-key', type=str,
                       help='MaxMind license key (or use MAXMIND_LICENSE_KEY env var)')

    args = parser.parse_args()

    # Check if database already exists
    if check_database():
        print("\nDatabase is ready to use!")
        return 0

    # Download mode
    if args.download or args.license_key:
        license_key = args.license_key or os.environ.get('MAXMIND_LICENSE_KEY')

        if not license_key:
            print("Error: No license key provided.")
            print("Set MAXMIND_LICENSE_KEY environment variable or use --license-key")
            return 1

        if download_with_license_key(license_key):
            return 0
        else:
            return 1

    # Show instructions
    download_instructions()
    return 1


if __name__ == '__main__':
    sys.exit(main())

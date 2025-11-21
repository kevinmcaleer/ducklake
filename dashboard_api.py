#!/usr/bin/env python
"""FastAPI service for serving DuckDB analytics dashboard data.

This service provides a public API endpoint for the kevsrobots.com stats page.
It runs scheduled updates of the DuckDB lakehouse and serves aggregated,
anonymized analytics data.
"""
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from pathlib import Path
import json
import logging
import os
import sys

from ducklake_core.dashboard_data import generate_dashboard_data
from ducklake_core.fetch_raw import fetch_page_count_postgres, fetch_search_logs_postgres
from ducklake_core.simple_pipeline import simple_refresh
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DB_PATH = os.getenv('DUCKDB_PATH', 'contentlake.ducklake')
UPDATE_INTERVAL_HOURS = int(os.getenv('UPDATE_INTERVAL_HOURS', '1'))
DASHBOARD_JSON_PATH = Path('dashboard_data.json')

# Create FastAPI app
app = FastAPI(
    title="KevsRobots Analytics Dashboard API",
    description="Public analytics API for kevsrobots.com statistics",
    version="1.0.0"
)

# Add CORS middleware to allow requests from kevsrobots.com
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.kevsrobots.com",
        "https://kevsrobots.com",
        "http://localhost:4000",  # For local Jekyll testing
        "http://local.kevsrobots.com:4000",  # For local development
        "http://127.0.0.1:4000",  # Alternative localhost
    ],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Global cache for dashboard data
_cached_dashboard_data = None
_cache_timestamp = None


def update_lakehouse():
    """Fetch latest data from Postgres and refresh DuckDB lakehouse.

    This runs on a schedule (hourly by default) to keep the data fresh.
    """
    try:
        logger.info("Starting lakehouse update...")

        # Fetch last 7 days from both sources
        logger.info("Fetching page_count data from Postgres...")
        page_count_result = fetch_page_count_postgres(
            overwrite=True,
            days=7
        )
        logger.info(f"Fetched {page_count_result.get('postgres', {}).get('rows_fetched', 0)} page_count records")

        logger.info("Fetching search_logs data from Postgres...")
        search_logs_result = fetch_search_logs_postgres(
            overwrite=True,
            days=7
        )
        logger.info(f"Fetched {search_logs_result.get('postgres', {}).get('rows_fetched', 0)} search_logs records")

        # Refresh DuckDB lakehouse
        logger.info("Refreshing DuckDB lakehouse...")
        con = duckdb.connect(DB_PATH)
        refresh_result = simple_refresh(con)
        con.close()
        logger.info("Lakehouse refresh complete")

        # Update dashboard data cache
        update_dashboard_cache()

        logger.info("Lakehouse update completed successfully")

    except Exception as e:
        logger.error(f"Error updating lakehouse: {e}", exc_info=True)


def update_dashboard_cache():
    """Generate and cache dashboard data.

    This is called after lakehouse updates and can also be triggered manually.
    """
    global _cached_dashboard_data, _cache_timestamp

    try:
        logger.info("Generating dashboard data...")
        data = generate_dashboard_data(DB_PATH)

        # Cache in memory
        _cached_dashboard_data = data
        _cache_timestamp = data['last_updated']

        # Also write to disk for persistence
        DASHBOARD_JSON_PATH.write_text(json.dumps(data, indent=2))

        logger.info(f"Dashboard data updated at {_cache_timestamp}")
        logger.info(f"  - Searches per day: {len(data['searches_per_day'])} days")
        logger.info(f"  - Total users (last month): {data['user_stats']['total_users']}")
        logger.info(f"  - Total visits (last month): {data['totals']['visits_last_month']}")

    except Exception as e:
        logger.error(f"Error generating dashboard data: {e}", exc_info=True)


@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup."""
    logger.info("Starting Dashboard API service...")

    # Load cached data from disk if available
    global _cached_dashboard_data
    if DASHBOARD_JSON_PATH.exists():
        try:
            _cached_dashboard_data = json.loads(DASHBOARD_JSON_PATH.read_text())
            logger.info("Loaded cached dashboard data from disk")
        except Exception as e:
            logger.warning(f"Could not load cached data: {e}")

    # Generate initial dashboard data
    if _cached_dashboard_data is None:
        logger.info("No cached data found, generating initial dashboard data...")
        update_dashboard_cache()

    # Set up scheduled updates
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        update_lakehouse,
        'cron',
        hour=f'*/{UPDATE_INTERVAL_HOURS}',  # Every N hours
        minute=0,
        id='lakehouse_update',
        replace_existing=True
    )
    scheduler.start()
    logger.info(f"Scheduled lakehouse updates every {UPDATE_INTERVAL_HOURS} hour(s)")

    # Store scheduler in app state so it can be shut down cleanly
    app.state.scheduler = scheduler


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    logger.info("Shutting down Dashboard API service...")
    if hasattr(app.state, 'scheduler'):
        app.state.scheduler.shutdown()


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "KevsRobots Analytics Dashboard API",
        "version": "1.0.0",
        "endpoints": {
            "/api/dashboard": "Get public dashboard data (aggregated, no PII)",
            "/api/health": "Health check endpoint",
            "/api/update": "Trigger manual update (admin only)"
        },
        "last_updated": _cache_timestamp,
        "update_interval": f"{UPDATE_INTERVAL_HOURS} hour(s)"
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring."""
    db_exists = Path(DB_PATH).exists()
    cache_exists = _cached_dashboard_data is not None

    return {
        "status": "healthy" if (db_exists and cache_exists) else "degraded",
        "database_exists": db_exists,
        "cache_loaded": cache_exists,
        "last_updated": _cache_timestamp
    }


@app.get("/api/dashboard")
async def get_dashboard(response: Response):
    """Get public dashboard data.

    Returns aggregated analytics data with NO personally identifiable information.
    All data is anonymized and aggregated for public consumption.

    Response includes:
    - Search trends and popular queries
    - Page view statistics
    - Device and OS breakdowns
    - User counts (anonymized)
    - Popular pages and content types
    """
    if _cached_dashboard_data is None:
        # Generate on-demand if not cached
        update_dashboard_cache()

    # Set cache headers (cache for 1 hour)
    response.headers["Cache-Control"] = "public, max-age=3600"

    return _cached_dashboard_data


@app.post("/api/update")
async def trigger_update():
    """Manually trigger a lakehouse update.

    This endpoint is for administrative use only. In production, you should
    add authentication/authorization middleware.
    """
    # TODO: Add authentication check
    logger.info("Manual update triggered via API")
    update_lakehouse()
    return {
        "status": "update_triggered",
        "message": "Lakehouse update started in background"
    }


if __name__ == "__main__":
    import uvicorn

    # Run the server
    port = int(os.getenv('PORT', '8001'))
    host = os.getenv('HOST', '0.0.0.0')

    logger.info(f"Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")

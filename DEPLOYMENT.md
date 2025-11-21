# DuckDB Lakehouse Analytics - Deployment Guide

This guide explains how to deploy the DuckDB Analytics Dashboard as a Docker container.

## Overview

The system provides:
- **FastAPI Dashboard API** - Public JSON endpoint for kevsrobots.com stats
- **Scheduled Updates** - Automatic hourly refreshes from Postgres
- **DuckDB Lakehouse** - Fast analytics on page views and search logs
- **No PII Exposure** - All data is aggregated and anonymized

## Architecture

```
┌─────────────────────────────────────────────┐
│  Docker Container: ducklake-analytics       │
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │  FastAPI Server (Port 8000)          │  │
│  │  - GET /api/dashboard (public)       │  │
│  │  - GET /api/health                   │  │
│  └──────────────────────────────────────┘  │
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │  APScheduler (Hourly Updates)        │  │
│  │  1. Fetch from Postgres              │  │
│  │  2. Refresh DuckDB                   │  │
│  │  3. Generate dashboard JSON          │  │
│  └──────────────────────────────────────┘  │
│                                             │
│  ┌──────────────────────────────────────┐  │
│  │  DuckDB Lakehouse                    │  │
│  │  - Raw data partitions               │  │
│  │  - Lake views                        │  │
│  │  - Aggregated reports                │  │
│  └──────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
         │
         ▼ Postgres Connections
┌─────────────────────┐    ┌─────────────────────┐
│  page_count DB      │    │  searchlogs DB      │
│  (192.168.2.3:5433) │    │  (192.168.2.3:5433) │
└─────────────────────┘    └─────────────────────┘
```

## Prerequisites

1. Docker and Docker Compose installed
2. Access to Postgres databases:
   - `page_count` database (visits table)
   - `searchlogs` database (search_logs table)
3. `.env` file with database credentials

## Setup

### 1. Create Environment File

Copy the example and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
# Postgres Configuration
DB_HOST=192.168.2.3
DB_PORT=5433
DB_USER=your_username
DB_PASSWORD=your_password

# Application Configuration
UPDATE_INTERVAL_HOURS=1
PORT=8000
```

**Important:** If your username or password contains special characters, URL-encode them:
- `:` → `%3A`
- `@` → `%40`
- `&` → `%26`
- `+` → `%2B`
- `#` → `%23`

### 2. Build and Start

```bash
# Build the container
docker-compose build

# Start the service
docker-compose up -d

# Check logs
docker-compose logs -f ducklake-analytics
```

### 3. Verify Deployment

```bash
# Check health
curl http://localhost:8002/api/health

# Get dashboard data
curl http://localhost:8002/api/dashboard
```

## API Endpoints

### `GET /api/dashboard`

Returns aggregated analytics data (JSON):
- Searches per day over last year
- Top searches (week/month)
- Search trends with movement indicators
- Device and OS breakdowns
- Popular pages and content types
- User statistics (anonymized)

**Example:**
```bash
curl http://localhost:8002/api/dashboard | jq
```

### `GET /api/health`

Health check endpoint for monitoring:

```json
{
  "status": "healthy",
  "database_exists": true,
  "cache_loaded": true,
  "last_updated": "2025-11-21T14:06:45Z"
}
```

### `POST /api/update`

Manually trigger a lakehouse update:

```bash
curl -X POST http://localhost:8002/api/update
```

**Note:** Should add authentication in production.

## Frontend Integration

The `stats_page_example.html` file provides a complete example of how to integrate the dashboard into your Jekyll site.

### Key Steps:

1. **Copy HTML to Jekyll:**
   ```bash
   cp stats_page_example.html /path/to/jekyll/site/stats.html
   ```

2. **Update API URL:**
   Change the API_URL constant in the JavaScript:
   ```javascript
   const API_URL = 'https://analytics.kevsrobots.com/api/dashboard';
   ```

3. **Configure CORS:**
   The API already allows requests from `kevsrobots.com`. If you need to add more domains, edit `dashboard_api.py`:
   ```python
   allow_origins=[
       "https://www.kevsrobots.com",
       "https://kevsrobots.com",
       "https://yourdomain.com",  # Add here
   ]
   ```

## Monitoring

### View Logs

```bash
# Follow logs in real-time
docker-compose logs -f ducklake-analytics

# View last 100 lines
docker-compose logs --tail=100 ducklake-analytics
```

### Check Container Status

```bash
docker-compose ps
```

### Monitor Updates

The service logs each update cycle:
```
INFO - Starting lakehouse update...
INFO - Fetched 9903 page_count records
INFO - Fetched 480 search_logs records
INFO - Lakehouse refresh complete
INFO - Dashboard data updated
```

## Maintenance

### Restart Service

```bash
docker-compose restart ducklake-analytics
```

### Update Code

```bash
# Pull latest code
git pull

# Rebuild and restart
docker-compose up -d --build
```

### Clear Data and Rebuild

```bash
# Stop service
docker-compose down

# Remove database and data
rm contentlake.ducklake
rm -rf data/

# Rebuild and start
docker-compose up -d --build
```

## Troubleshooting

### Issue: "Connection refused" to Postgres

**Solution:** Check that Postgres is accessible from the container:
```bash
docker-compose exec ducklake-analytics ping -c 3 192.168.2.3
```

### Issue: "Dashboard data not updating"

**Solution:** Check scheduler logs:
```bash
docker-compose logs ducklake-analytics | grep "lakehouse update"
```

### Issue: "High memory usage"

**Solution:** DuckDB can use significant memory for large datasets. Add memory limits in `docker-compose.yml`:
```yaml
services:
  ducklake-analytics:
    mem_limit: 2g
    memswap_limit: 2g
```

## Performance Tuning

### Adjust Update Frequency

Edit `.env`:
```bash
UPDATE_INTERVAL_HOURS=2  # Update every 2 hours instead of 1
```

### Limit Data Retention

The current setup fetches last 7 days on each update. To reduce this, edit `dashboard_api.py`:
```python
# Change from days=7 to days=1
fetch_page_count_postgres(overwrite=True, days=1)
```

## Security Considerations

1. **Database Credentials:** Store in `.env`, never commit to git
2. **CORS Configuration:** Limit to your domains only
3. **API Authentication:** Add auth middleware for `/api/update` endpoint
4. **Network Isolation:** Run on private network, expose only necessary ports
5. **HTTPS:** Use a reverse proxy (nginx/traefik) with SSL certificates

## Production Deployment

For production, consider:

1. **Reverse Proxy:**
   ```nginx
   server {
       server_name analytics.kevsrobots.com;
       location / {
           proxy_pass http://localhost:8002;
       }
   }
   ```

2. **SSL Certificate:**
   ```bash
   certbot --nginx -d analytics.kevsrobots.com
   ```

3. **Systemd Service:**
   Create `/etc/systemd/system/ducklake-analytics.service` to auto-start on boot

4. **Monitoring:**
   Set up health check monitoring (e.g., UptimeRobot, Pingdom)

## Support

For issues or questions:
- Check logs: `docker-compose logs ducklake-analytics`
- Review API health: `curl http://localhost:8002/api/health`
- Inspect data: `docker-compose exec ducklake-analytics ls -la /app/data`

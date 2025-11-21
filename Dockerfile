# Dockerfile for DuckDB Lakehouse Analytics Service
# Provides FastAPI dashboard endpoint and scheduled data updates

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ducklake_core/ ./ducklake_core/
COPY sql/ ./sql/
COPY dashboard_api.py .
COPY run_refresh.py .

# Create directories for data persistence
RUN mkdir -p /app/data/raw /app/data/lake /app/reports

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DUCKDB_PATH=/app/contentlake.ducklake
ENV UPDATE_INTERVAL_HOURS=1
ENV PORT=8000
ENV HOST=0.0.0.0

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Run the dashboard API service
CMD ["python", "dashboard_api.py"]

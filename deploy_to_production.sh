#!/bin/bash
# Deploy updated dashboard to production server (192.168.2.4)

echo "Connecting to production server to deploy updated dashboard..."
ssh pi@192.168.2.4 << 'ENDSSH'
cd /home/pi/ducklake || exit 1

echo "Pulling latest code from git..."
git pull

echo "Rebuilding Docker container with updated CORS settings..."
docker compose down
docker compose build --no-cache
docker compose up -d

echo "Checking container status..."
docker compose ps

echo "Deployment complete!"
ENDSSH

# ====================================================================
# Restart-Clean.ps1
# --------------------------------------------------------------------
# This script safely rebuilds and restarts your entire Docker stack.
# It ensures your latest Python code (like refresh_summaries.py)
# is copied into the container and prevents restart loops.
# ====================================================================

Write-Host "🧹 Stopping all containers and removing old volumes..." -ForegroundColor Yellow
docker compose down -v

Write-Host "🔨 Rebuilding all Docker images (no cache)..." -ForegroundColor Cyan
docker compose build --no-cache

Write-Host "🚀 Starting all services in detached mode..." -ForegroundColor Green
docker compose up -d

Write-Host "⏳ Waiting for containers to initialize..." -ForegroundColor DarkYellow
Start-Sleep -Seconds 10

Write-Host "🔍 Checking container status..." -ForegroundColor Cyan
docker ps

Write-Host "✅ Clean rebuild complete! Containers are up and running." -ForegroundColor Green

Write-Host "`n🔎 To monitor ETL logs live, run:" -ForegroundColor DarkYellow
Write-Host "docker compose logs -f etl" -ForegroundColor Cyan

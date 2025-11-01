🚀 SKU Analysis Backend — Production Deployment Guide
Repository: Ai-firelab/SKU_Analysis_Backend (dev3 branch) Stack: FastAPI · PostgreSQL · Docker · Python ETL Maintainer: AI FireLab Data Engineering Team

🧠 Overview
This project provides a production-grade backend for Procurement SKU Frequency & Spend Analysis.

It includes:

⚙️ ETL Pipeline: Robust ingestion with checkpointing, retries, and resumable state.

🧩 Analytics API: FastAPI-based endpoints powered by Materialized Views.

🕵️ Monitoring & Alerts: Email notifications for ETL failures or anomalies.

🐳 Dockerized Infrastructure: API + ETL + PostgreSQL deployed seamlessly.


📁 Project Structure

SKU_Analysis_Backend/
│
├── docker-compose.yml  # Main orchestration (API, ETL, DB)
├── Dockerfile          # Base image definition
├── .env.example        # Safe environment template
├── .gitignore          # Excludes secrets/logs
├── requirements.txt    # Dependencies
├── scripts/            # Cron and scheduling scripts
│   ├── run_etl_daily.ps1
│   └── run_etl_daily.sh
│
├── src/
│   ├── api/            # FastAPI app (main, routes, middleware)
│   ├── etl/            # ETL pipeline (resilient ingestion)
│   ├── db/             # Schema, materialized views, refresh logic
│   ├── monitoring/     # Alerts, watchdogs, test alerting
│   └── common/         # Shared utility modules
│
└── docker-init/
    └── init.sql        # DB bootstrap (auto-created on startup)


⚙️ Setup Instructions

1. Clone the repository
   git clone -b dev3 https://github.com/Ai-firelab/SKU_Analysis_Backend.git
cd SKU_Analysis_Backend

2.Create your environment file
   Copy-Item .env.example .env
3.Edit .env to set credentials
   DATABASE_URL=postgresql://postgres:password@postgres:5432/procurementdb
   DATA_SOURCE_URL=https://procurement-sku-analysis-mock.onrender.com/purchase-orders
   ENABLE_EMAIL_ALERTS=true
   ALERT_EMAILS=yourname@company.com
   SMTP_USER=youremail@gmail.com
   SMTP_PASSWORD=your_app_password

🐳 Docker Deployment
 1. Build and start containers
    docker compose up -d --build
 2.Check running containers
    docker ps
 3. Check logs
    docker compose logs -f api
    docker compose logs -f etl
 4. When successful:
    procurement_api ... Up (port 8000)
    procurement_etl ... Exited (success)
    procurementdb ... Up (port 5432)

▶️ Manual ETL Ingestion
Historical (Full Load)
PowerShell
docker compose run --rm etl python src/etl/etl_ingest_resilient.py

Daily Incremental
Edit .env:

Ini, TOML

MODE=daily
HISTORICAL_TRUNCATE=false
Then:

PowerShell

docker compose restart etl
🕒 Automated ETL Scheduling (Phase 5C.3)
Windows PowerShell + Task Scheduler

1. Create file: scripts/run_etl_daily.ps1

PowerShell

cd "C:\path\to\SKU_Analysis_Backend"
docker compose run --rm etl python src/etl/etl_ingest_resilient.py
2. Then in Task Scheduler:

Action → Start a program

Program: powershell.exe

Arguments: -File "C:\path\to\SKU_Analysis_Backend\scripts\run_etl_daily.ps1"

Trigger: Daily at 2:00 AM

✅ Your ETL will run automatically each night and send alerts if issues occur.

🔔 Monitoring & Alerts (Phase 2.9)
Alerts are handled by src/monitoring/alerting.py and include:

❌ ETL failure

⏱️ Runtime exceeds threshold

⚠️ Zero rows processed

💤 No success in last 24 hours

Test alerts manually:

PowerShell

docker compose run --rm etl python src/monitoring/test_alerting.py
If setup correctly, you’ll receive test emails at addresses in ALERT_EMAILS.

🧩 FastAPI Analytics API
Once Docker is running:

API URL → http://127.0.0.1:8000

Swagger Docs → http://127.0.0.1:8000/docs

Redoc → http://127.0.0.1:8000/redoc

Health Check → http://127.0.0.1:8000/api/v1/health

Example request:

PowerShell

Invoke-WebRequest http://127.0.0.1:8000/api/v1/sku/top?limit=10
🧠 Database Access & Verification
Access PostgreSQL inside Docker:

PowerShell

docker exec -it procurementdb psql -U postgres -d procurementdb
Basic checks:

SQL

\dt
SELECT COUNT(*) FROM purchase_orders;
SELECT COUNT(*) FROM mv_sku_spend;
🧹 Maintenance Tasks
Run PostgreSQL maintenance weekly:

PowerShell

docker exec -it procurementdb psql -U postgres -d procurementdb -f src/db/performance_maintenance.sql
This performs:

VACUUM + ANALYZE

REINDEX

Refresh Materialized Views

Summarize table stats

🔐 Security Best Practices
❌ Never commit .env — only .env.example.

Use App Passwords for Gmail SMTP.

Rotate SMTP credentials periodically.

Use Docker volumes for persistent PostgreSQL storage.

Restrict database access in production.

🧰 Common PowerShell Commands
Action,Command
Build all containers,docker compose build
Start all containers,docker compose up -d
Stop containers,docker compose down
Restart ETL only,docker compose restart etl
Show ETL logs,docker compose logs -f etl
Show API logs,docker compose logs -f api
Access PostgreSQL shell,docker exec -it procurementdb psql -U postgres -d procurementdb

⚠️ Troubleshooting Guide
Issue,Cause,Fix
"relation ""purchase_orders"" does not exist",DB not initialized,Run docker-init/init.sql
ETL stuck on chunk,Database lock,Restart DB container
Email alert not working,Wrong SMTP credentials,Use Gmail App Password
API returns 500,DB connection error,Check API logs
ETL retrying constantly,Source API rate-limited,Increase RATE_LIMIT_DELAY in .env


Quick Reference Summary
Start stack:

PowerShell

docker compose up -d --build
Run ETL manually:

PowerShell

docker compose run --rm etl python src/etl/etl_ingest_resilient.py
Open API docs: http://127.0.0.1:8000/docs

Check DB:

PowerShell

docker exec -it procurementdb psql -U postgres -d procurementdb
Automated run (daily 2 AM): Add to Task Scheduler → scripts/run_etl_daily.ps1

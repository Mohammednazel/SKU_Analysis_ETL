# 🚀 SKU Analysis Backend — Production Deployment Guide

**Repository:** [Ai-firelab/SKU_Analysis_Backend (dev3 branch)](https://github.com/Ai-firelab/SKU_Analysis_Backend/tree/dev3)  
**Stack:** FastAPI · PostgreSQL · Docker · Python ETL  
**Maintainer:** AI FireLab Data Engineering Team  

---

## 🧠 Overview

This project provides a **production-grade backend** for Procurement SKU Frequency & Spend Analysis.  
It includes:

- ⚙️ **ETL Pipeline:** Robust ingestion with checkpointing, retries, and resumable state.  
- 🧩 **Analytics API:** FastAPI-based endpoints powered by Materialized Views.  
- 🕵️ **Monitoring & Alerts:** Email notifications for ETL failures or anomalies.  
- 🐳 **Dockerized Infrastructure:** API + ETL + PostgreSQL deployed seamlessly.

---

## 📁 Project Structure

SKU_Analysis_Backend/
│
├── docker-compose.yml # Main orchestration (API, ETL, DB)
├── Dockerfile # Base image definition
├── .env.example # Safe environment template
├── .gitignore # Excludes secrets/logs
├── requirements.txt # Dependencies
├── scripts/ # Cron and scheduling scripts
│ ├── run_etl_daily.ps1
│ └── run_etl_daily.sh
│
├── src/
│ ├── api/ # FastAPI app (main, routes, middleware)
│ ├── etl/ # ETL pipeline (resilient ingestion)
│ ├── db/ # Schema, materialized views, refresh logic
│ ├── monitoring/ # Alerts, watchdogs, test alerting
│ └── common/ # Shared utility modules
│
└── docker-init/
└── init.sql # DB bootstrap (auto-created on startup)

yaml
Copy code

---

## ⚙️ Setup Instructions

### 🪟 For Windows PowerShell

1. **Clone the repository**
   ```powershell
   git clone -b dev3 https://github.com/Ai-firelab/SKU_Analysis_Backend.git
   cd SKU_Analysis_Backend
Create your environment file

powershell
Copy code
Copy-Item .env.example .env
Edit .env to set credentials

env
Copy code
DATABASE_URL=postgresql://postgres:password@postgres:5432/procurementdb
DATA_SOURCE_URL=https://procurement-sku-analysis-mock.onrender.com/purchase-orders
ENABLE_EMAIL_ALERTS=true
ALERT_EMAILS=yourname@company.com
SMTP_USER=youremail@gmail.com
SMTP_PASSWORD=your_app_password
🐳 Docker Deployment
Build and start containers

powershell
Copy code
docker compose up -d --build
Check running containers

powershell
Copy code
docker ps
Check logs

powershell
Copy code
docker compose logs -f api
docker compose logs -f etl
When successful:

scss
Copy code
procurement_api       ... Up (port 8000)
procurement_etl       ... Exited (success)
procurementdb          ... Up (port 5432)
▶️ Manual ETL Ingestion
Historical (Full Load)
powershell
Copy code
docker compose run --rm etl python src/etl/etl_ingest_resilient.py
Daily Incremental
Edit .env:

ini
Copy code
MODE=daily
HISTORICAL_TRUNCATE=false
Then:

powershell
Copy code
docker compose restart etl
🕒 Automated ETL Scheduling (Phase 5C.3)
Windows PowerShell + Task Scheduler
Create file: scripts/run_etl_daily.ps1

powershell
Copy code
cd "C:\path\to\SKU_Analysis_Backend"
docker compose run --rm etl python src/etl/etl_ingest_resilient.py
Then in Task Scheduler:

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
powershell
Copy code
docker compose run --rm etl python src/monitoring/test_alerting.py
If setup correctly, you’ll receive test emails at addresses in ALERT_EMAILS.

🧩 FastAPI Analytics API
Once Docker is running:

API URL → http://127.0.0.1:8000

Swagger Docs → http://127.0.0.1:8000/docs

Redoc → http://127.0.0.1:8000/redoc

Health Check → http://127.0.0.1:8000/api/v1/health

Example request:

powershell
Copy code
Invoke-WebRequest http://127.0.0.1:8000/api/v1/sku/top?limit=10
🧠 Database Access & Verification
Access PostgreSQL inside Docker:

powershell
Copy code
docker exec -it procurementdb psql -U postgres -d procurementdb
Basic checks:

sql
Copy code
\dt
SELECT COUNT(*) FROM purchase_orders;
SELECT COUNT(*) FROM mv_sku_spend;
🧹 Maintenance Tasks
Run PostgreSQL maintenance weekly:

powershell
Copy code
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
Action	Command
Build all containers	docker compose build
Start all containers	docker compose up -d
Stop containers	docker compose down
Restart ETL only	docker compose restart etl
Show ETL logs	docker compose logs -f etl
Show API logs	docker compose logs -f api
Access PostgreSQL shell	docker exec -it procurementdb psql -U postgres -d procurementdb

⚠️ Troubleshooting Guide
Issue	Cause	Fix
relation "purchase_orders" does not exist	DB not initialized	Run docker-init/init.sql
ETL stuck on chunk	Database lock	Restart DB container
Email alert not working	Wrong SMTP credentials	Use Gmail App Password
API returns 500	DB connection error	Check API logs
ETL retrying constantly	Source API rate-limited	Increase RATE_LIMIT_DELAY in .env

📄 License & Ownership
© AI FireLab Data Engineering Team
Lead Developer: Mohammed Nazel
Branch: dev3
Environment: Dockerized PostgreSQL + FastAPI + Python ETL

For internal use within AI FireLab — not for public redistribution.

✅ Quick Reference Summary
Start stack:

powershell
Copy code
docker compose up -d --build
Run ETL manually:

powershell
Copy code
docker compose run --rm etl python src/etl/etl_ingest_resilient.py
Open API docs:

arduino
Copy code
http://127.0.0.1:8000/docs
Check DB:

powershell
Copy code
docker exec -it procurementdb psql -U postgres -d procurementdb
Automated run (daily 2 AM):

Add to Task Scheduler → scripts/run_etl_daily.ps1
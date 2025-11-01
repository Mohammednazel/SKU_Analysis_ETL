# Procurement SKU Frequency & Spend Analysis (Backend)

## 🧠 Overview
A production-ready backend for procurement data analysis:
- ETL ingestion pipeline with checkpointing, resilience, and monitoring
- FastAPI analytics API (materialized view powered)
- Dockerized stack (PostgreSQL + ETL + API)
- Email alerts on ETL failure/slow/zero-row events

## ⚙️ Components
- **src/etl/** → resilient ETL pipeline  
- **src/api/** → FastAPI application  
- **src/db/** → SQL schema, views, and maintenance scripts  
- **src/monitoring/** → alerting and watchdog modules  

## 🚀 Quick Start
```bash
docker compose up --build


ETL will populate data → API available at:

http://127.0.0.1:8000/docs


📦 Environment

Copy .env.example → .env and set:

DATABASE_URL=postgresql://postgres:password@postgres:5432/procurementdb
MODE=daily
ENABLE_EMAIL_ALERTS=false


🧩 Useful Commands
# Manual ETL run
docker compose run --rm etl python src/etl/etl_ingest_resilient.py

# Check API logs
docker compose logs -f api

# Enter DB shell
docker exec -it procurementdb psql -U postgres -d procurementdb

🔔 Monitoring

Email alerts sent via Phase 2.9 alerting module.
See src/monitoring/test_alerting.py for test cases.
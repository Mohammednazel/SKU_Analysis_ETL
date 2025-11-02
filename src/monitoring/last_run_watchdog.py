# Detects stale or missing ETL runs

import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from monitoring.alerting import send_email

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)

# Alert if last success older than N hours
THRESH_HOURS = int(os.getenv("WATCHDOG_MAX_AGE_HOURS", "24"))

def main():
    q = text("""
        SELECT end_time
        FROM etl_run_log
        WHERE status='success'
        ORDER BY run_id DESC
        LIMIT 1
    """)
    with ENGINE.connect() as conn:
        last_end = conn.execute(q).scalar()

    now = datetime.now(timezone.utc)
    if not last_end or (now - last_end).total_seconds() > THRESH_HOURS * 3600:
        subj = f"[ETL Watchdog] No successful runs in last {THRESH_HOURS}h"
        body = f"Last successful run at: {last_end}\nNow: {now}\nAction required."
        send_email(subj, body, None)

if __name__ == "__main__":
    main()

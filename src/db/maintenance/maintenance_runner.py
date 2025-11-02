# Python wrapper for maintenance SQL (Phase 3F)

"""
---------------------------------------------------------
Automated Database Maintenance Runner (Phase 3F)
---------------------------------------------------------
 Purpose:
    - Automatically run SQL maintenance scripts 
      after a set number of successful ETL runs.
    - Keeps database fast, vacuumed, indexed, and updated.
    - Designed for PostgreSQL.

 Features:
    - Reads DATABASE_URL from .env
    - Checks last ETL run logs from `etl_run_log`
    - Runs SQL script via psycopg2 or SQLAlchemy
    - Tracks last maintenance timestamp in `etl_checkpoint`
---------------------------------------------------------
"""

import os
import subprocess
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# =========================================================
# 1️⃣ Configuration
# =========================================================
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
MAINTENANCE_SQL_PATH = os.path.join("src", "db", "performance_maintenance.sql")
MAINTENANCE_THRESHOLD = int(os.getenv("MAINTENANCE_RUN_EVERY", 10))  # run every 10 ETL successes

LOG_DIR = "src/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "maintenance_runner.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================================================
# 2️⃣ Database Connection
# =========================================================
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS maintenance_tracker (
            id SERIAL PRIMARY KEY,
            last_run TIMESTAMP WITH TIME ZONE DEFAULT now(),
            total_successes INT DEFAULT 0
        );
    """))


# =========================================================
# 3️⃣ Utility: Get ETL Run Stats
# =========================================================
def get_last_runs():
    """Fetch last 20 ETL runs to decide if maintenance is due."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT run_id, start_time, end_time, status
            FROM etl_run_log
            ORDER BY run_id DESC
            LIMIT 20;
        """))
        return result.fetchall()


def get_last_success_count():
    """Count total successful ETL runs since last maintenance checkpoint."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS maintenance_tracker (
                id SERIAL PRIMARY KEY,
                last_run TIMESTAMP WITH TIME ZONE DEFAULT now(),
                total_successes INT DEFAULT 0
            );
        """))

        last_entry = conn.execute(text("SELECT total_successes FROM maintenance_tracker ORDER BY id DESC LIMIT 1;")).scalar()
        return int(last_entry or 0)


def update_maintenance_checkpoint(success_count):
    """Save maintenance checkpoint after successful run."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO maintenance_tracker (last_run, total_successes)
            VALUES (now(), :scount)
        """), {"scount": success_count})


# =========================================================
# 4️⃣ Core Maintenance Runner
# =========================================================
def run_maintenance_script():
    """Execute maintenance SQL directly using SQLAlchemy instead of psql."""
    try:
        logger.info("Starting maintenance script (via SQLAlchemy)...")

        if not os.path.exists(MAINTENANCE_SQL_PATH):
            raise FileNotFoundError(f"Maintenance SQL not found: {MAINTENANCE_SQL_PATH}")

        with open(MAINTENANCE_SQL_PATH, "r", encoding="utf-8") as f:
            sql_script = f.read()

        with engine.begin() as conn:
            for stmt in sql_script.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        logger.info("✅ Maintenance SQL executed successfully using SQLAlchemy.")

    except Exception as e:
        logger.exception(f" Maintenance script failed: {e}")


# =========================================================
# 5️⃣ Main Logic: Decide When to Run
# =========================================================
def main():
    logger.info(" Checking ETL run log for maintenance trigger...")

    # Get all successful ETL runs
    with engine.connect() as conn:
        total_successes = conn.execute(text("""
            SELECT COUNT(*) FROM etl_run_log WHERE status = 'success';
        """)).scalar()

    last_success_checkpoint = get_last_success_count()
    new_successes = total_successes - last_success_checkpoint

    logger.info(f"Total successful ETL runs: {total_successes}, "
                f"since last maintenance: {new_successes}, "
                f"threshold: {MAINTENANCE_THRESHOLD}")

    if new_successes >= MAINTENANCE_THRESHOLD:
        logger.info("🧹 Maintenance threshold reached. Running maintenance SQL...")
        run_maintenance_script()
        update_maintenance_checkpoint(total_successes)
    else:
        logger.info(" Maintenance not needed yet.")


# =========================================================
# Entry Point
# =========================================================
if __name__ == "__main__":
    main()

"""
Production-Grade ETL Ingestion Pipeline (Phase 2.5)
---------------------------------------------------
- Pagination with has_more
- Incremental vs Historical modes (ENV: MODE=daily|historical)
- Checkpoint resume (etl_checkpoint table)
- Optional truncate on historical (ENV: HISTORICAL_TRUNCATE=true|false)
- Hash-aware UPSERT (update only when values changed)
- Chunked DB writes, retries/backoff, rate limiting, session pooling
"""

import sys , os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import json
import time
import hashlib
import logging
import pandas as pd
import requests
from datetime import datetime, timezone
from typing import Optional, Tuple

from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.db.refresh_summaries import refresh_all


# =========================================================
# 1) Configuration
# =========================================================
load_dotenv()

DATABASE_URL       = os.getenv("DATABASE_URL")
DATA_SOURCE_URL    = os.getenv("DATA_SOURCE_URL")
CHUNK_SIZE         = min(int(os.getenv("CHUNK_SIZE", 100)), 100)  # API caps at 100
RATE_LIMIT_DELAY   = float(os.getenv("RATE_LIMIT_DELAY", 0.2))    # seconds between API calls
INCREMENTAL_DAYS   = int(os.getenv("INCREMENTAL_DAYS", 2))        # days lookback for daily loads
MODE               = os.getenv("MODE", "daily").lower()           # "daily" or "historical"
HIST_TRUNCATE_ENV  = os.getenv("HISTORICAL_TRUNCATE", "true").lower()
HISTORICAL_TRUNCATE = HIST_TRUNCATE_ENV in ("1", "true", "yes")

JOB_NAME           = os.getenv("JOB_NAME", "purchase_order_ingest")
TARGET_TABLE       = os.getenv("TARGET_TABLE", "purchase_orders")

# =========================================================
# 2) Logging
# =========================================================
LOG_DIR = "src/logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "etl_ingest.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================================================
# 3) DB Engine & Metadata
# =========================================================
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)
metadata = MetaData()

# =========================================================
# 4) One-time bootstrap helpers
# =========================================================

# table stores the last successfully processed offset
# (i.e., how far the ETL got before stopping). So if your ETL stops midway — e.g., after processing 60,000 of 100,000 records — next time it starts, it picks up from where it left off.
def ensure_checkpoint_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS etl_checkpoint (
        id SERIAL PRIMARY KEY,
        job_name TEXT UNIQUE NOT NULL,
        last_offset INT NOT NULL DEFAULT 0,
        last_run TIMESTAMP WITH TIME ZONE DEFAULT now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# Reads the last saved offset
def get_checkpoint(job_name: str) -> int:
    with engine.connect() as conn:
        val = conn.execute(
            text("SELECT last_offset FROM etl_checkpoint WHERE job_name = :job"),
            {"job": job_name},
        ).scalar()
        return int(val or 0)

# Updates or inserts the latest offset after every successful chunk
def save_checkpoint(job_name: str, offset: int):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etl_checkpoint (job_name, last_offset, last_run)
                VALUES (:job, :off, now())
                ON CONFLICT (job_name)
                DO UPDATE SET last_offset = EXCLUDED.last_offset, last_run = EXCLUDED.last_run;
            """),
            {"job": job_name, "off": offset},
        )

def record_etl_run(start_time, end_time, rows_processed, status, error_msg=None, mode: str = "daily"):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etl_run_log
                (mode, start_time, end_time, rows_processed, rows_inserted, status, error_message)
                VALUES (:mode, :start, :end, :rp, :ri, :st, :err)
            """),
            {
                "mode": mode,
                "start": start_time,
                "end": end_time,
                "rp": rows_processed,
                "ri": rows_processed,  # counting processed here; split if you track exact inserts
                "st": status,
                "err": error_msg,
            },
        )

def optional_truncate_for_historical():
    if MODE == "historical" and HISTORICAL_TRUNCATE:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE} RESTART IDENTITY;"))
        save_checkpoint(JOB_NAME, 0)
        logger.info("Table truncated for historical reload (HISTORICAL_TRUNCATE=true) and checkpoint reset to 0.")


# =========================================================
# 5) HTTP Session (keep-alive + retries on 5xx)
# =========================================================
session = requests.Session()
retries = Retry(
    total=5, backoff_factor=0.5,
    status_forcelist=(500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)

# =========================================================
# 6) Extract: Paginated fetch (with optional start_date)
# =========================================================
@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_page(offset: int = 0, limit: int = CHUNK_SIZE, start_date: Optional[datetime] = None) -> Tuple[list, dict]:
    params = {"limit": limit, "offset": offset}
    if start_date:
        params["start_date"] = start_date.strftime("%Y-%m-%d")

    resp = session.get(DATA_SOURCE_URL, params=params, timeout=60)
    resp.raise_for_status()
    js = resp.json()

    items = js.get("purchase_orders") or js.get("data") or []
    pagination = js.get("pagination", {})
    return items, pagination

# =========================================================
# 7) Transform: flatten JSON → DataFrame
# =========================================================
def transform_data(raw_orders: list) -> pd.DataFrame:
    flat = []
    for order in raw_orders:
        base = {
            "purchase_order_id": order.get("purchase_order_id"),
            "created_date":     order.get("created_date"),
            "status":           order.get("status"),
            "supplier_id":      order.get("supplier_id"),
            "purchasing_group": order.get("purchasing_group"),
        }
        for item in order.get("items", []):
            row = {
                **base,
                "line_item_number": item.get("item_number"),
                "plant":            item.get("plant"),
                "product_id":       item.get("product_id"),
                "description":      item.get("description"),
                "quantity":         pd.to_numeric(item.get("quantity"), errors="coerce"),
                "unit":             item.get("unit"),
                "unit_price":       pd.to_numeric(item.get("unit_price"), errors="coerce"),
                "net_value":        pd.to_numeric(item.get("net_value"), errors="coerce"),
                "material_group":   item.get("material_group"),
            }
            # hash for idempotency & change detection
            row["source_hash"] = hashlib.md5(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
            flat.append(row)

    df = pd.DataFrame(flat)
    if not df.empty:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    return df

# =========================================================
# 8) Load: UPSERT only when changed (hash-aware), in batches
# =========================================================
def upsert_dataframe(df: pd.DataFrame, table_name: str = TARGET_TABLE, chunk_size: int = 2000):
    if df.empty:
        return
    table = Table(table_name, metadata, autoload_with=engine)
    total = len(df)
    with engine.begin() as conn:
        for start in range(0, total, chunk_size):
            batch = df.iloc[start:start + chunk_size]
            records = batch.to_dict(orient="records")
            stmt = insert(table).values(records)

            # Update all columns except PKs only if source_hash differs
            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name not in ("purchase_order_id", "line_item_number")
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=["purchase_order_id", "line_item_number"],
                set_=update_cols,
                where=(table.c.source_hash != stmt.excluded.source_hash)  # <— D) update only when changed
            )
            conn.execute(stmt)
    logger.info(f"Upserted {total} rows.")

# =========================================================
# 9) Orchestrator
# =========================================================
def run_etl():
    ensure_checkpoint_table()
    if MODE == "historical":
        optional_truncate_for_historical()

    start_time = datetime.now(timezone.utc)
    total_processed = 0
    status = "success"
    error_msg = None

    logger.info(f"ETL started in {MODE.upper()} mode.")

    # daily → incremental by date; historical → full dump (no start_date)
    start_date = None if MODE == "historical" else datetime.now(timezone.utc) - pd.Timedelta(days=INCREMENTAL_DAYS)

    # use checkpoint for both modes to enable resume
    offset = 0 if MODE == "historical" else get_checkpoint(JOB_NAME)

    try:
        while True:
            data, pagination = fetch_page(offset=offset, limit=CHUNK_SIZE, start_date=start_date)
            logger.info(f"Fetched batch: rows={len(data)}, offset={offset}, has_more={pagination.get('has_more')}")

            if not data:
                logger.info("No more data to process.")
                break

            df = transform_data(data)
            upsert_dataframe(df)
            total_processed += len(df)

            # move offset and persist checkpoint
            step = pagination.get("returned", len(data))
            offset += step
            save_checkpoint(JOB_NAME, offset)

            logger.info(f"Processed chunk; new_offset={offset}, total_so_far={total_processed}")

            if not pagination.get("has_more"):
                break

            time.sleep(RATE_LIMIT_DELAY)

    except Exception as e:
        logger.exception("ETL failed:")
        status = "failed"
        error_msg = str(e)

    finally:
        # close the session cleanly (E)
        try:
            session.close()
        except Exception:
            pass

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        record_etl_run(start_time, end_time, total_processed, status, error_msg, MODE)
        logger.info(f"ETL finished | Mode={MODE} | Status={status} | Duration={duration:.2f}s | Rows={total_processed}")
        if status == "success" and os.getenv("REFRESH_SUMMARIES", "true").lower() in ("1", "true", "yes"):
            refresh_all(
                concurrently=True,
                snapshot=os.getenv("TAKE_DAILY_SNAPSHOT", "false").lower() in ("1", "true", "yes")
        )



if __name__ == "__main__":
    run_etl()

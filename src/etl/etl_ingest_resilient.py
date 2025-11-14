##############################################
# ETL INGEST (Optimized with ThreadPool Fetch)
# API-SPEC SAFE (limit <= 100)
##############################################

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import json
import time
import hashlib
import logging
import pandas as pd
import requests
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, Table, MetaData, text, inspect
from sqlalchemy.dialects.postgresql import insert
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.db.refresh_summaries import refresh_all
from monitoring.alerting import send_email
from monitoring.monitors import evaluate_run

# =========================================================
# 1) Configuration
# =========================================================

from dotenv import load_dotenv
load_dotenv()

DATABASE_URL        = os.getenv("DATABASE_URL")
DATA_SOURCE_URL     = os.getenv("DATA_SOURCE_URL")

# MUST respect API spec limit <= 100
CHUNK_SIZE          = min(int(os.getenv("CHUNK_SIZE", "100")), 100)

# NEW: parallel fetching workers
FETCH_WORKERS       = int(os.getenv("FETCH_WORKERS", "6"))

RATE_LIMIT_DELAY    = float(os.getenv("RATE_LIMIT_DELAY", "0.05"))
INCREMENTAL_DAYS    = int(os.getenv("INCREMENTAL_DAYS", "2"))
MODE                = (os.getenv("MODE", "daily") or "daily").lower()
HIST_TRUNCATE_ENV   = (os.getenv("HISTORICAL_TRUNCATE", "true") or "true").lower()
HISTORICAL_TRUNCATE = HIST_TRUNCATE_ENV in ("1", "true", "yes")

_JOB_BASE_NAME      = os.getenv("JOB_NAME", "purchase_order_ingest")
JOB_NAME            = f"{_JOB_BASE_NAME}_{MODE}"
TARGET_TABLE        = os.getenv("TARGET_TABLE", "purchase_orders")

REFRESH_SUMMARIES   = (os.getenv("REFRESH_SUMMARIES", "true") or "true").lower() in ("1", "true", "yes")
TAKE_DAILY_SNAPSHOT = (os.getenv("TAKE_DAILY_SNAPSHOT", "false") or "false").lower() in ("1", "true", "yes")

# =========================================================
# 2) Logging
# =========================================================
LOG_DIR = "src/logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "etl_ingest.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =========================================================
# 3) Config validation
# =========================================================
def validate_env() -> None:
    missing = [k for k in ["DATABASE_URL", "DATA_SOURCE_URL"] if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")

validate_env()


# =========================================================
# 4) DB Engine
# =========================================================
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_timeout=60
)
metadata = MetaData()


# =========================================================
# 5) Checkpoint + Lock Helpers
# =========================================================

def ensure_checkpoint_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS etl_checkpoint (
        job_name TEXT PRIMARY KEY,
        last_offset INT NOT NULL DEFAULT 0,
        last_run TIMESTAMPTZ DEFAULT now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def get_checkpoint(job_name: str) -> int:
    with engine.connect() as conn:
        v = conn.execute(text(
            "SELECT last_offset FROM etl_checkpoint WHERE job_name = :job"),
            {"job": job_name}).scalar()
        return int(v or 0)


def save_checkpoint(job_name: str, offset: int):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_checkpoint (job_name, last_offset, last_run)
            VALUES (:job, :off, now())
            ON CONFLICT (job_name)
            DO UPDATE SET last_offset = EXCLUDED.last_offset, last_run = EXCLUDED.last_run;
        """), {"job": job_name, "off": offset})


def acquire_lock(job_name: str):
    """Assumes etl_lock exists (in init.sql)"""
    with engine.begin() as conn:
        existing = conn.execute(text(
            "SELECT 1 FROM etl_lock WHERE job_name=:job AND status='running'"),
            {"job": job_name}).fetchone()
        if existing:
            raise RuntimeError(f"ETL job '{job_name}' is already running.")

        conn.execute(text("""
            INSERT INTO etl_lock (job_name, started_at, status)
            VALUES (:job, now(), 'running')
            ON CONFLICT (job_name)
            DO UPDATE SET started_at = now(), status='running';
        """), {"job": job_name})


def release_lock(job_name: str):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM etl_lock WHERE job_name=:job"), {"job": job_name})
    logger.info(f"🔓 Lock released for {job_name}")


def record_etl_run(start_time, end_time, rows_processed, status, error_msg=None, mode: str = "daily"):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_run_log
            (mode, start_time, end_time, rows_processed, rows_inserted, status, error_message)
            VALUES (:mode, :start, :end, :rp, :ri, :st, :err)
        """), {
            "mode": mode,
            "start": start_time, "end": end_time,
            "rp": rows_processed, "ri": rows_processed,
            "st": status, "err": error_msg
        })


def optional_truncate_for_historical():
    if MODE == "historical" and HISTORICAL_TRUNCATE:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE} RESTART IDENTITY;"))
        save_checkpoint(JOB_NAME, 0)
        logger.info("Historical mode: table truncated, checkpoint reset.")


# =========================================================
# 6) Requests Session (Retry + Pooling)
# =========================================================

def make_session():
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=40, max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# =========================================================
# 7) ThreadPool Parallel Fetch IMPLEMENTATION
# =========================================================

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5),
       retry=retry_if_exception_type((requests.exceptions.RequestException,)))
def fetch_offset(session, offset: int, limit: int, params_extra: dict):
    params = {"limit": limit, "offset": offset}
    if params_extra:
        params.update(params_extra)

    resp = session.get(DATA_SOURCE_URL, params=params, timeout=40)

    if resp.status_code == 429:
        ra = int(resp.headers.get("Retry-After", "5"))
        logger.warning(f"429 - sleeping {ra}s")
        time.sleep(ra)
        raise requests.exceptions.RequestException("429 Too Many Requests")

    resp.raise_for_status()
    js = resp.json()

    # universal normalization
    items = js.get("purchase_orders") or js.get("data") or []
    pagination = js.get("pagination", {}) or {}

    returned = pagination.get("returned", len(items))
    has_more = pagination.get("has_more", bool(items))

    return {
        "offset": offset,
        "items": items,
        "returned": returned,
        "has_more": has_more
    }


def parallel_fetch(start_offset: int, limit: int, start_date: Optional[datetime]):
    """
    ThreadPool parallel fetching using offset pagination.
    Yields: {"offset":X, "items":[...], "returned":N, "has_more":bool}
    """
    params_extra = {}
    if start_date:
        params_extra["start_date"] = start_date.strftime("%Y-%m-%d")

    session = make_session()

    offsets_queue = list(range(start_offset, start_offset + (FETCH_WORKERS * limit), limit))
    max_pages = 2000  # safety cap

    submitted = {}
    page_counter = 0

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        # submit initial
        for off in offsets_queue:
            fut = ex.submit(fetch_offset, session, off, limit, params_extra)
            submitted[fut] = off

        next_offset = offsets_queue[-1] + limit

        while submitted:
            for fut in as_completed(list(submitted.keys())):
                off = submitted.pop(fut)
                try:
                    result = fut.result()
                except Exception as e:
                    logger.exception(f"Fetch failed for offset {off}: {e}")
                    continue

                yield result
                page_counter += 1

                if not result["has_more"]:
                    logger.info("No more pages indicated by API.")
                    return

                # submit next offset
                if page_counter < max_pages:
                    fut2 = ex.submit(fetch_offset, session, next_offset, limit, params_extra)
                    submitted[fut2] = next_offset
                    next_offset += limit

    try:
        session.close()
    except:
        pass


# =========================================================
# 8) Transform
# =========================================================

CRITICAL_FIELDS_ORDER = ["purchase_order_id"]
CRITICAL_FIELDS_ITEM  = ["item_number", "product_id"]

def transform_page(orders: list) -> pd.DataFrame:
    flat = []
    for order in orders:
        if any(not order.get(k) for k in CRITICAL_FIELDS_ORDER):
            continue

        base = {
            "purchase_order_id": order.get("purchase_order_id"),
            "created_date":     order.get("created_date"),
            "status":           order.get("status"),
            "supplier_id":      order.get("supplier_id"),
            "purchasing_group": order.get("purchasing_group"),
        }

        for item in order.get("items", []):
            if any(item.get(k) in (None, "") for k in CRITICAL_FIELDS_ITEM):
                continue

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

            hash_basis = {
                k: row.get(k)
                for k in [
                    "purchase_order_id","line_item_number","product_id",
                    "quantity","unit_price","net_value",
                    "supplier_id","created_date"
                ]
            }
            row["source_hash"] = hashlib.md5(
                json.dumps(hash_basis, sort_keys=True, default=str).encode()
            ).hexdigest()

            flat.append(row)

    df = pd.DataFrame(flat)
    if not df.empty:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce", utc=True)
        df = df.dropna(subset=["purchase_order_id", "line_item_number", "product_id"])
    return df


# =========================================================
# 9) Load (UPSERT)
# =========================================================

def upsert_dataframe(df: pd.DataFrame, table_name: str = TARGET_TABLE, chunk_size: int = 2000):
    if df.empty:
        return 0
    table = Table(table_name, metadata, autoload_with=engine)

    total = len(df)
    inserted = 0

    with engine.begin() as conn:
        for start in range(0, total, chunk_size):
            batch = df.iloc[start:start+chunk_size]
            records = batch.to_dict(orient="records")
            stmt = insert(table).values(records)

            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name not in ("purchase_order_id", "line_item_number")
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=["purchase_order_id", "line_item_number"],
                set_=update_cols,
                where=(table.c.source_hash != stmt.excluded.source_hash)
            )
            conn.execute(stmt)
            inserted += len(records)

    logger.info(f"Upserted: {inserted} rows.")
    return inserted


# =========================================================
# 10) Main ETL Orchestrator (UPDATED for Parallel Fetch)
# =========================================================

def run_etl():
    ensure_checkpoint_table()
    optional_truncate_for_historical()

    start_time = datetime.now(timezone.utc)
    total_processed = 0
    status = "success"
    error_msg = None

    logger.info(f"🚀 ETL START | Mode={MODE}")

    start_date = None if MODE == "historical" else datetime.now(timezone.utc) - pd.Timedelta(days=INCREMENTAL_DAYS)
    offset = 0 if MODE == "historical" else get_checkpoint(JOB_NAME)

    try:
        acquire_lock(JOB_NAME)

        for page in parallel_fetch(offset, CHUNK_SIZE, start_date):
            items = page["items"]
            if not items:
                logger.info("Empty page, skipping.")
                continue

            df = transform_page(items)
            inserted = upsert_dataframe(df)
            total_processed += inserted

            offset += page["returned"]
            save_checkpoint(JOB_NAME, offset)

            logger.info(f"Processed offset={page['offset']}, returned={page['returned']}, total={total_processed}")

            if not page["has_more"]:
                break

            time.sleep(RATE_LIMIT_DELAY)

    except Exception as e:
        logger.exception("ETL failed:")
        status = "failed"
        error_msg = str(e)

    finally:
        release_lock(JOB_NAME)

        end_time = datetime.now(timezone.utc)
        record_etl_run(start_time, end_time, total_processed, status, error_msg, MODE)

        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL FINISHED | Status={status} | Rows={total_processed} | Duration={duration:.2f}s")

        ok, issues = evaluate_run(engine, MODE, total_processed, duration)
        should_email = (status != "success") or not ok

        if should_email:
            lines = [
                f"Job: {JOB_NAME}",
                f"Mode: {MODE}",
                f"Status: {status}",
                f"Rows processed: {total_processed}",
                f"Duration: {duration:.1f}s",
                f"Last offset: {offset}"
            ]
            if error_msg:
                lines.append(f"Error: {error_msg}")
            if issues:
                lines.append("Issues:")
                lines += [f" - {i}" for i in issues]

            send_email(
                f"[ETL] {status.upper()} - rows={total_processed}",
                "\n".join(lines),
                "<br>".join(lines)
            )

        if status == "success" and REFRESH_SUMMARIES:
            refresh_all(
                concurrently=True,
                snapshot=TAKE_DAILY_SNAPSHOT
            )


if __name__ == "__main__":
    run_etl()

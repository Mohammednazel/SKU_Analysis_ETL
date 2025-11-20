##############################################
# ETL INGEST (Optimized with ThreadPool Fetch)
# API-SPEC SAFE (limit <= 100)
##############################################

import sys, os


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


def get_checkpoint(job_name: str) -> Tuple[int, Optional[datetime]]:
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT last_offset, last_run FROM etl_checkpoint WHERE job_name = :job"),
            {"job": job_name}).fetchone()
        
        if row:
            # Return (offset, last_run_timestamp)
            return int(row[0]), row[1]
        else:
            return 0, None

def save_checkpoint(job_name: str, offset: int):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_checkpoint (job_name, last_offset, last_run)
            VALUES (:job, :off, now())
            ON CONFLICT (job_name)
            DO UPDATE SET last_offset = EXCLUDED.last_offset, last_run = EXCLUDED.last_run;
        """), {"job": job_name, "off": offset})


def acquire_lock(job_name: str):
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT started_at FROM etl_lock WHERE job_name=:job AND status='running'"),
            {"job": job_name}
        ).fetchone()

        if row:
            started_at = row[0]
            age_minutes = (datetime.now(timezone.utc) - started_at).total_seconds() / 60

            if age_minutes > 30:
                logger.warning(f"⚠ Stale ETL lock found (age {age_minutes:.1f} min). Auto-clearing.")
                conn.execute(text("DELETE FROM etl_lock WHERE job_name=:job"), {"job": job_name})
            else:
                raise RuntimeError(f"ETL job '{job_name}' already running.")
        
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
        # submit initial batch of parallel requests
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

                # ----------------------------------------------------
                # NEW: #3 PAGINATION SAFETY CHECK
                # ----------------------------------------------------
                returned = result["returned"]
                has_more = result["has_more"]

                if returned < limit and has_more:
                    logger.warning(
                        f"⚠ Pagination mismatch: returned={returned} < limit={limit} "
                        f"but has_more=True | offset={off}"
                    )
                # ----------------------------------------------------

                yield result
                page_counter += 1

                # stop when API says no more data
                if not result["has_more"]:
                    logger.info("No more pages indicated by API.")
                    return

                # submit next offset if limit not exceeded
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

# --- optimized transform_page ---
import numpy as np

TRANSFORM_BATCH_MIN_ROWS_FOR_CATEGORY = 1000  # only convert to category when many rows

def transform_page_fast(orders: list) -> pd.DataFrame:
    """
    Build list-of-tuples and create DataFrame from it for speed.
    Compute 'source_hash' using a single concatenated string column then list-comprehension md5.
    """
    rows = []  # list of tuples ordered to match `cols`
    # Order of columns (tuple order) — explicit to create DataFrame faster
    cols = [
        "purchase_order_id", "line_item_number", "created_date", "status",
        "supplier_id", "purchasing_group", "plant", "product_id", "description",
        "quantity", "unit", "unit_price", "net_value", "material_group"
    ]

    for order in orders:
        po_id = order.get("purchase_order_id")
        if not po_id:
            continue
        base_created = order.get("created_date")
        base_status = order.get("status")
        base_supplier = order.get("supplier_id")
        base_pgroup = order.get("purchasing_group")

        items = order.get("items") or []
        for item in items:
            item_no = item.get("item_number")
            product = item.get("product_id")
            if item_no in (None, "") or not product:
                continue
            # parse numeric safely
            try:
                qty = float(item.get("quantity") or 0)
            except Exception:
                qty = None
            try:
                unit_price = float(item.get("unit_price") or 0)
            except Exception:
                unit_price = None
            try:
                net_val = float(item.get("net_value") or 0)
            except Exception:
                net_val = None

            rows.append((
                po_id,
                item_no,
                base_created,
                base_status,
                base_supplier,
                base_pgroup,
                item.get("plant"),
                product,
                item.get("description"),
                qty,
                item.get("unit"),
                unit_price,
                net_val,
                item.get("material_group"),
            ))

    if not rows:
        return pd.DataFrame(columns=cols + ["source_hash"])

    # Create DataFrame with explicit dtypes where possible
    df = pd.DataFrame(rows, columns=cols)

    # Coerce dtypes
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["net_value"] = pd.to_numeric(df["net_value"], errors="coerce")
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce", utc=True)

    # Build concat string using vectorized str methods — faster than Python concat per row
    # Use fillna('') to avoid NAs
    str_cols = ["purchase_order_id", "line_item_number", "product_id",
                "quantity", "unit_price", "net_value", "supplier_id", "created_date"]
    # Convert numeric columns to string once (faster)
    for c in ["quantity", "unit_price", "net_value"]:
        df[c] = df[c].fillna("").map(lambda x: ("{:.6f}".format(x) if isinstance(x, (int, float)) else str(x)))

    df["created_date"] = df["created_date"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

    # Now use pandas vectorized str.cat progressively to avoid axis=1 agg overhead
    concat = df["purchase_order_id"].astype(str)
    for c in ["line_item_number", "product_id", "quantity", "unit_price", "net_value", "supplier_id", "created_date"]:
        concat = concat.str.cat(df[c].astype(str), sep="|")

    # Compute MD5 via list comprehension (fast in CPython because hashlib is C)
    import hashlib
    concat_list = concat.tolist()
    hashes = [hashlib.md5(s.encode("utf-8")).hexdigest() for s in concat_list]
    df["source_hash"] = hashes

    # Optional: convert low-cardinality columns to category to save memory if chunk is large
    if len(df) >= TRANSFORM_BATCH_MIN_ROWS_FOR_CATEGORY:
        for c in ["supplier_id", "purchasing_group", "material_group", "plant"]:
            if c in df.columns:
                df[c] = df[c].astype("category")

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
    
    # Historical truncation (if enabled)
    optional_truncate_for_historical() 

    start_time = datetime.now(timezone.utc)
    total_processed = 0
    status = "success"
    error_msg = None

    logger.info(f"🚀 ETL START | Job={JOB_NAME} | Mode={MODE}") 

    start_date = None if MODE == "historical" else datetime.now(timezone.utc) - pd.Timedelta(days=INCREMENTAL_DAYS)
    
    # --- NEW LOGIC START ---
    # Get both the offset AND the last time it ran
    stored_offset, last_run_ts = get_checkpoint(JOB_NAME)
    
    # Get current UTC date
    today_date = datetime.now(timezone.utc).date()

    if MODE == "daily":
        # If there was a previous run AND it was on a day BEFORE today:
        # It means this is a fresh daily run. Reset to 0.
        if last_run_ts and last_run_ts.date() < today_date:
            logger.info(f"🔄 New day detected (Last run: {last_run_ts.date()}). Resetting offset to 0.")
            offset = 0
            # Immediately save the 0 checkpoint so if we crash at offset 50, 
            # we resume from 50 (since date will now be today)
            save_checkpoint(JOB_NAME, 0)
        else:
            # If last_run was today (resume from crash) OR never ran (first time)
            logger.info(f"⏯️ Resuming today's run from offset {stored_offset}.")
            offset = stored_offset
    else:
        # Historical mode always resumes where it left off until finished
        offset = stored_offset

    logger.info(f"Starting execution from offset: {offset}")
    # --- NEW LOGIC END ---

    try:
        acquire_lock(JOB_NAME) 

        for page in parallel_fetch(offset, CHUNK_SIZE, start_date):
            items = page["items"]
            if not items:
                logger.info("Empty page, skipping.")
                continue

            # Use the fast transform
            df = transform_page_fast(items)

            inserted = upsert_dataframe(df)
            total_processed += inserted
            
            # Advance offset based on items returned
            current_page_offset = page["offset"]
            new_offset = current_page_offset + page["returned"]
            
            save_checkpoint(JOB_NAME, new_offset) 

            logger.info(f"Processed offset={page['offset']}, returned={page['returned']}, total={total_processed}, next_offset={new_offset}")

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
        logger.info(f"ETL FINISHED | Job={JOB_NAME} | Status={status} | Rows={total_processed} | Duration={duration:.2f}s")

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
                f"[ETL] {status.upper()} - {JOB_NAME}",
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

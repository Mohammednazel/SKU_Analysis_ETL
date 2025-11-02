# Full production-grade ETL ingestion (Phase 2.8)


"""
Production-Grade ETL Ingestion Pipeline (Phase 2.8, resilient)
--------------------------------------------------------------
- Pagination with has_more
- Incremental vs Historical (MODE=daily|historical)
- Checkpoint resume (etl_checkpoint)
- Optional truncate on historical (HISTORICAL_TRUNCATE=true|false)
- Hash-aware UPSERT (update only when values changed)
- Chunked DB writes, retries/backoff, rate limiting, session pooling
- NEW (2.8): Config validation, ETL lock, 429 handling, response validation,
             target table validation, basic data quality checks
"""

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

from sqlalchemy import create_engine, Table, MetaData, text, inspect
from sqlalchemy.dialects.postgresql import insert
from tenacity import retry, wait_exponential, stop_after_attempt

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

DATABASE_URL        = os.getenv("DATABASE_URL")
DATA_SOURCE_URL     = os.getenv("DATA_SOURCE_URL")
CHUNK_SIZE          = min(int(os.getenv("CHUNK_SIZE", "100")), 100)  # API caps at 100
RATE_LIMIT_DELAY    = float(os.getenv("RATE_LIMIT_DELAY", "0.2"))    # seconds between API calls
INCREMENTAL_DAYS    = int(os.getenv("INCREMENTAL_DAYS", "2"))        # for daily loads
MODE                = (os.getenv("MODE", "daily") or "daily").lower()  # daily|historical
HIST_TRUNCATE_ENV   = (os.getenv("HISTORICAL_TRUNCATE", "true") or "true").lower()
HISTORICAL_TRUNCATE = HIST_TRUNCATE_ENV in ("1", "true", "yes")

JOB_NAME            = os.getenv("JOB_NAME", "purchase_order_ingest")
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
# 3) Config validation (NEW)
# =========================================================
def validate_env() -> None:
    missing = [k for k in ["DATABASE_URL", "DATA_SOURCE_URL", "TARGET_TABLE", "MODE"] if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")

    if MODE not in ("daily", "historical"):
        raise RuntimeError(f"MODE must be 'daily' or 'historical', got: {MODE}")

    # numeric checks already cast above; raise if invalid literals
    try:
        _ = int(os.getenv("CHUNK_SIZE", "100"))
        _ = float(os.getenv("RATE_LIMIT_DELAY", "0.2"))
        _ = int(os.getenv("INCREMENTAL_DAYS", "2"))
    except ValueError as ve:
        raise RuntimeError(f"Invalid numeric environment value: {ve}")

validate_env()

# =========================================================
# 4) DB Engine & Metadata
# =========================================================
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)
metadata = MetaData()

def validate_target_table(expected_cols: List[str]) -> None:
    """Ensure TARGET_TABLE exists and has required columns."""
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if TARGET_TABLE not in tables:
        raise RuntimeError(f"Target table '{TARGET_TABLE}' does not exist.")
    cols = {c["name"] for c in inspector.get_columns(TARGET_TABLE)}
    missing = [c for c in expected_cols if c not in cols]
    if missing:
        raise RuntimeError(f"Target table '{TARGET_TABLE}' missing columns: {missing}")

# =========================================================
# 5) One-time bootstrap helpers (checkpoint + lock)
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
        v = conn.execute(
            text("SELECT last_offset FROM etl_checkpoint WHERE job_name = :job"),
            {"job": job_name},
        ).scalar()
        return int(v or 0)

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
                "ri": rows_processed,  # if you later compute "exact inserts", split these
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

# ---- ETL lock (NEW) ----
def acquire_lock(job_name: str):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_lock (
                job_name TEXT PRIMARY KEY,
                started_at TIMESTAMPTZ DEFAULT now(),
                status TEXT DEFAULT 'running'
            )
        """))
        existing = conn.execute(
            text("SELECT 1 FROM etl_lock WHERE job_name=:job AND status='running'"),
            {"job": job_name}
        ).fetchone()
        if existing:
            raise RuntimeError(f"ETL job '{job_name}' is already running.")
        conn.execute(
            text("""
                INSERT INTO etl_lock (job_name, started_at, status)
                VALUES (:job, now(), 'running')
                ON CONFLICT (job_name) DO UPDATE SET started_at = now(), status='running';
            """),
            {"job": job_name}
        )
    logger.info(f"🔒 ETL lock acquired for {job_name}.")

def release_lock(job_name: str):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM etl_lock WHERE job_name=:job"), {"job": job_name})
    logger.info(f"🔓 ETL lock released for {job_name}.")

# =========================================================
# 6) HTTP Session (keep-alive + retries including 429)  (UPDATED)
# =========================================================
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),  # include 429
    allowed_methods=frozenset(["GET"]),
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)

# =========================================================
# 7) Extract: Paginated fetch with response validation (UPDATED)
# =========================================================
def _validate_api_json(js: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not isinstance(js, dict):
        raise ValueError("Invalid API response: expected object at top-level.")
    items = js.get("purchase_orders") or js.get("data") or []
    if not isinstance(items, list):
        raise ValueError("Invalid API response: 'purchase_orders'/'data' must be a list.")
    pagination = js.get("pagination", {}) or {}
    if not isinstance(pagination, dict):
        pagination = {}
    # normalize pagination keys
    if "returned" not in pagination:
        pagination["returned"] = len(items)
    if "has_more" not in pagination:
        pagination["has_more"] = bool(items)
    return items, pagination

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def fetch_page(offset: int = 0, limit: int = CHUNK_SIZE, start_date: Optional[datetime] = None) -> Tuple[list, dict]:
    params = {"limit": limit, "offset": offset}
    if start_date:
        params["start_date"] = start_date.strftime("%Y-%m-%d")

    resp = session.get(DATA_SOURCE_URL, params=params, timeout=60)

    # Handle hard rate limits explicitly
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "5"))
        logger.warning(f"429 Too Many Requests. Sleeping for {retry_after}s before retry.")
        time.sleep(retry_after)
        # raise to trigger tenacity retry
        resp.raise_for_status()

    resp.raise_for_status()
    js = resp.json()
    items, pagination = _validate_api_json(js)
    return items, pagination

# =========================================================
# 8) Transform: flatten JSON → DataFrame (with basic data quality checks) (UPDATED)
# =========================================================
CRITICAL_FIELDS_ORDER = ["purchase_order_id"]
CRITICAL_FIELDS_ITEM  = ["item_number", "product_id"]

def transform_data(raw_orders: list) -> pd.DataFrame:
    flat = []
    for order in raw_orders:
        # basic check: skip orders without an ID
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
            # skip items missing critical keys
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
            # hash for idempotency & change detection
            # (stable fields only → exclude description if it's noisy)
            hash_basis = {
                k: row.get(k)
                for k in [
                    "purchase_order_id", "line_item_number", "product_id",
                    "quantity", "unit_price", "net_value", "supplier_id", "created_date"
                ]
            }
            row["source_hash"] = hashlib.md5(json.dumps(hash_basis, sort_keys=True, default=str).encode()).hexdigest()
            flat.append(row)

    df = pd.DataFrame(flat)
    if not df.empty:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce", utc=True)
        # drop rows with missing PKs just in case
        df = df.dropna(subset=["purchase_order_id", "line_item_number", "product_id"])
    return df

# =========================================================
# 9) Load: UPSERT only when changed (hash-aware), in batches
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

            # Update all cols except PKs only if source_hash differs
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
    logger.info(f"Upserted {total} rows.")

# =========================================================
# 10) Orchestrator
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
    start_date = None if MODE == "historical" else datetime.now(timezone.utc) - pd.Timedelta(days=INCREMENTAL_DAYS)
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
        try:
            session.close()
        except Exception:
            pass

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        record_etl_run(start_time, end_time, total_processed, status, error_msg, MODE)
        logger.info(f"ETL finished | Mode={MODE} | Status={status} | Duration={duration:.2f}s | Rows={total_processed}")

        # ✅ Evaluate & alert (only necessary logic, no duplication)
        ok, issues = evaluate_run(engine, MODE, total_processed, duration)
        should_email = (status != "success") or (not ok)

        if should_email:
            # Build a compact email
            subj = f"[ETL] {'FAILED' if status!='success' else 'Attention'} - mode={MODE}, rows={total_processed}, dur={duration:.1f}s"
            lines = [
                f"Job: {JOB_NAME}",
                f"Mode: {MODE}",
                f"Status: {status}",
                f"Rows processed: {total_processed}",
                f"Duration: {duration:.1f} sec",
                f"Offset (last): {offset}",
            ]
            if error_msg:
                lines.append(f"Error: {error_msg}")
            if issues:
                lines.append("Heuristics:")
                lines += [f" - {i}" for i in issues]

            text_body = "\n".join(lines)
            html_body = "<br>".join(lines).replace(" - ", "&nbsp;&nbsp;• ")

            send_email(subj, text_body, html_body)

        # Optional: success summary (kept off by default)
        if os.getenv("ALERT_ON_SUCCESS_SUMMARY", "false").lower() in ("1", "true", "yes") and status == "success":
            send_email(
                f"[ETL] Success - mode={MODE}, rows={total_processed}",
                f"Job {JOB_NAME} completed successfully.\nRows: {total_processed}\nDuration: {duration:.1f}s",
                None
            )

        if status == "success" and os.getenv("REFRESH_SUMMARIES", "true").lower() in ("1", "true", "yes"):
            refresh_all(
                concurrently=True,
                snapshot=os.getenv("TAKE_DAILY_SNAPSHOT", "false").lower() in ("1", "true", "yes")
            )


if __name__ == "__main__":
    run_etl()

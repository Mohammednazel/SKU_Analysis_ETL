# extract/common/batch_manager.py
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from extract.common.db_utils import get_db_connection

logger = logging.getLogger(__name__)

# --- CONFIG FOR PILOT RUN (3 Months) ---
# We use date(2024, 4, 1) as END because it means "Up to, but not including April"
BACKFILL_START = date(2024, 1, 1)
BACKFILL_END   = date(2024, 4, 1) 

def ensure_batch_table_exists():
    """Creates the state tracking table if it doesn't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_core.etl_batches (
                    batch_id SERIAL PRIMARY KEY,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status TEXT DEFAULT 'PENDING', -- PENDING, IN_PROGRESS, COMPLETED, FAILED
                    files_count INT DEFAULT 0,
                    rows_inserted INT DEFAULT 0,
                    error_message TEXT,
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_dates 
                ON app_core.etl_batches(start_date, end_date);
            """)
        conn.commit()
    finally:
        conn.close()

def initialize_batches():
    """Populates the table with monthly slices if empty."""
    ensure_batch_table_exists()
    
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Check if table is populated. 
        # If populated, we DO NOT add more rows (to prevent duplicates/mess).
        cur.execute("SELECT count(*) FROM app_core.etl_batches")
        if cur.fetchone()[0] > 0:
            logger.info("ℹ️ Batches already initialized. Skipping creation.")
            return

        logger.info(f"📅 Initializing batches from {BACKFILL_START} to {BACKFILL_END}...")
        
        current = BACKFILL_START
        batch_rows = []
        
        while current < BACKFILL_END:
            next_month = current + relativedelta(months=1)
            # Tuple: (start_date, end_date)
            batch_rows.append((current, next_month))
            current = next_month

        sql = """
            INSERT INTO app_core.etl_batches (start_date, end_date, status)
            VALUES (%s, %s, 'PENDING')
            ON CONFLICT DO NOTHING;
        """
        cur.executemany(sql, batch_rows)
        conn.commit()
        logger.info(f"✅ Created {len(batch_rows)} monthly batches.")
        
    finally:
        conn.close()

def get_next_batch():
    """Locks and returns the next PENDING batch."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Find oldest PENDING batch
        cur.execute("""
            SELECT batch_id, start_date, end_date 
            FROM app_core.etl_batches 
            WHERE status = 'PENDING' 
            ORDER BY start_date ASC 
            LIMIT 1 
            FOR UPDATE SKIP LOCKED
        """)
        row = cur.fetchone()
        
        if row:
            batch_id, start_d, end_d = row
            # Mark as IN_PROGRESS
            cur.execute("""
                UPDATE app_core.etl_batches 
                SET status = 'IN_PROGRESS', updated_at = now() 
                WHERE batch_id = %s
            """, (batch_id,))
            conn.commit()
            return {"id": batch_id, "start": start_d, "end": end_d}
        
        return None  # No more work!
    finally:
        conn.close()

def mark_batch_complete(batch_id, files_count, rows_inserted):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app_core.etl_batches 
                SET status = 'COMPLETED', 
                    files_count = %s, 
                    rows_inserted = %s, 
                    updated_at = now() 
                WHERE batch_id = %s
            """, (files_count, rows_inserted, batch_id))
        conn.commit()
    finally:
        conn.close()

def mark_batch_failed(batch_id, error_msg):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app_core.etl_batches 
                SET status = 'FAILED', 
                    error_message = %s, 
                    updated_at = now() 
                WHERE batch_id = %s
            """, (str(error_msg), batch_id))
        conn.commit()
    finally:
        conn.close()
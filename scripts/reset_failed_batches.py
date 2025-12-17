# reset_failed_batches.py
import os
from dotenv import load_dotenv  # <--- NEW
load_dotenv()                   # <--- NEW: Loads DB_PASS from .env

from extract.common.db_utils import get_db_connection

def reset_batches():
    print("🔌 Connecting to Database...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check how many failed first
        cur.execute("SELECT count(*) FROM app_core.etl_batches WHERE status = 'FAILED'")
        failed_count = cur.fetchone()[0]
        print(f"🧐 Found {failed_count} FAILED batches.")

        if failed_count > 0:
            # Reset them
            cur.execute("UPDATE app_core.etl_batches SET status = 'PENDING' WHERE status = 'FAILED'")
            conn.commit()
            print(f"✅ Successfully reset {failed_count} batches to 'PENDING'.")
        else:
            print("✨ No failed batches found. You are good to go.")
            
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    reset_batches()
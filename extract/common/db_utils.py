# extract/common/db_utils.py
import os
import psycopg2

def get_db_connection():
    """
    Establishes a connection to the Azure PostgreSQL database.
    Checks for DB_PASSWORD first, then falls back to DB_PASS.
    """
    # 1. Try to find the password using both common names
    db_password = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS")
    
    if not db_password:
        raise ValueError("❌ Database Password not found! Check DB_PASS or DB_PASSWORD env vars.")

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),  # Use the found password
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        raise e
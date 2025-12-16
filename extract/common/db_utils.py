# extract/common/db_utils.py
import os
import psycopg2

def get_db_connection():
    """
    Establishes a connection to the Azure PostgreSQL database
    using environment variables.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        raise e
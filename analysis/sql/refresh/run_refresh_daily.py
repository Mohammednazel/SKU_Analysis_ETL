import os
import psycopg2

# Get Config from Env
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Path to the SQL file (Relative to this script)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_FILE_PATH = os.path.join(CURRENT_DIR, "refresh_daily.sql")

def run_refresh():
    print(f"🔄 Connecting to {DB_HOST}...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        conn.autocommit = True
        cursor = conn.cursor()

        print(f"📖 Reading SQL from {SQL_FILE_PATH}...")
        with open(SQL_FILE_PATH, "r") as f:
            sql_commands = f.read()

        print("🚀 Executing Refresh SQL...")
        cursor.execute(sql_commands)
        
        print("✅ Refresh Complete.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error during refresh: {e}")
        # Don't exit(1) here if you want the pipeline to finish even if refresh fails
        # But for data integrity, exiting is usually better:
        exit(1)

if __name__ == "__main__":
    run_refresh()
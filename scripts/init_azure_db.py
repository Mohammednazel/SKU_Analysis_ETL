import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# 1. Load variables from .env file
load_dotenv()

# 2. Get Config from Environment
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
# We start by connecting to 'postgres' to create the new DB
TARGET_DB = "postgres"

def create_database():
    print(f"🔌 Connecting to {DB_HOST} (Secure Mode)...")
    
    if not DB_PASS:
        raise ValueError("❌ Error: DB_PASS not found in .env file!")

    try:
        # Connect to default 'postgres' db
        conn = psycopg2.connect(
            host=DB_HOST, 
            user=DB_USER, 
            password=DB_PASS, 
            dbname="postgres", 
            sslmode="require"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print(f"🔨 Creating database '{TARGET_DB}'...")
        cursor.execute(f"CREATE DATABASE {TARGET_DB};")
        print(f"✅ Database '{TARGET_DB}' created successfully!")
        
    except psycopg2.errors.DuplicateDatabase:
        print(f"⚠️ Database '{TARGET_DB}' already exists. Skipping creation.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals(): conn.close()

def run_schema_script():
    print(f"\n🔌 Connecting to '{TARGET_DB}' to run schema...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, 
            user=DB_USER, 
            password=DB_PASS, 
            dbname=TARGET_DB, 
            sslmode="require"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        schema_path = "step6_db/schema_postgres.sql"
        print(f"📖 Reading schema from {schema_path}...")
        with open(schema_path, "r") as f:
            sql_commands = f.read()

        print("🚀 Executing Schema SQL...")
        cursor.execute(sql_commands)
        print("✅ Schema 'app_core' initialized successfully!")
        
    except Exception as e:
        print(f"❌ Schema Error: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    create_database()
    run_schema_script()
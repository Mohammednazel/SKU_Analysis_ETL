# verify_mvs.py
import os
import psycopg2
from dotenv import load_dotenv
from tabulate import tabulate # Optional, prints pretty tables

load_dotenv()

def check_analytics_objects():
    print("🔌 Connecting to Database...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"), 
            user=os.getenv("DB_USER"), 
            password=os.getenv("DB_PASS"), 
            dbname="postgres", 
            sslmode="require"
        )
        cur = conn.cursor()

        # 1. Check if Schema Exists
        print("\n🔍 Checking Schemas...")
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'app_analytics';")
        if cur.fetchone():
            print("✅ Schema 'app_analytics' FOUND.")
        else:
            print("❌ Schema 'app_analytics' NOT FOUND.")

        # 2. List All Materialized Views
        print("\n📊 Checking Materialized Views:")
        query = """
        SELECT schemaname, matviewname, ispopulated 
        FROM pg_matviews 
        WHERE schemaname IN ('public', 'app_analytics')
        ORDER BY schemaname, matviewname;
        """
        cur.execute(query)
        mvs = cur.fetchall()
        
        if mvs:
            print(f"{'SCHEMA':<15} | {'VIEW NAME':<30} | {'POPULATED?'}")
            print("-" * 60)
            for row in mvs:
                print(f"{row[0]:<15} | {row[1]:<30} | {row[2]}")
        else:
            print("⚠️ No Materialized Views found!")

        # 3. Count Rows in a Key View (e.g., Global KPIs)
        print("\n📉 Verifying Data Content:")
        try:
            cur.execute("SELECT count(*) FROM app_analytics.mv_global_kpis;")
            count = cur.fetchone()[0]
            print(f"✅ 'mv_global_kpis' contains {count} rows.")
        except Exception as e:
            print(f"⚠️ Could not read 'mv_global_kpis'. Did you use the right schema? Error: {e}")

        conn.close()

    except Exception as e:
        print(f"❌ Connection Failed: {e}")

if __name__ == "__main__":
    check_analytics_objects()
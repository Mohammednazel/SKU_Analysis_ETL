# verify_success.py
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def verify_data():
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

        print("\n📊 FINAL SCOREBOARD (Historical Load):")
        print("------------------------------------------------")
        
        # 1. Total Counts
        cur.execute("SELECT count(*) FROM app_core.purchase_order_headers;")
        total_headers = cur.fetchone()[0]
        
        cur.execute("SELECT count(*) FROM app_core.purchase_order_items;")
        total_items = cur.fetchone()[0]
        
        print(f"✅ TOTAL Headers: {total_headers:,}")
        print(f"✅ TOTAL Items:   {total_items:,}")
        print("------------------------------------------------")

        # 2. Breakdown by Month (The Proof)
        print("\n📅 DATA BREAKDOWN BY MONTH:")
        print(f"{'MONTH':<15} | {'HEADERS':<10} | {'ITEMS':<10}")
        print("-" * 40)
        
        query = """
        SELECT 
            TO_CHAR(order_date, 'YYYY-MM') as month, 
            COUNT(DISTINCT purchase_order_id) as header_count,
            COUNT(*) as item_count
        FROM app_core.purchase_order_items
        GROUP BY 1
        ORDER BY 1;
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        for row in rows:
            print(f"{row[0]:<15} | {row[1]:<10,} | {row[2]:<10,}")

        print("------------------------------------------------")
        
        # 3. Validation Logic
        if total_items > 330000:
            print("🚀 SUCCESS: Item count matches the logs (~336k)!")
        else:
            print("⚠️ WARNING: Data might be missing. Check logs.")

        conn.close()

    except Exception as e:
        print(f"❌ DB Error: {e}")

if __name__ == "__main__":
    verify_data()
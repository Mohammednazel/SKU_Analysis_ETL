import os
import time
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()

# Define the exact order of execution (Dependency Chain)
SQL_EXECUTION_ORDER = [
    # 1. Base Logic (Views)
    "analysis/sql/base_views/v_headers_enriched.sql",
    "analysis/sql/base_views/v_items_enriched.sql",

    # 2. Aggregation Layers (Base MVs)
    "analysis/sql/materialized_views/mv_global_kpis.sql",
    "analysis/sql/materialized_views/mv_sku_monthly_metrics.sql",
    "analysis/sql/materialized_views/mv_sku_weekly_metrics.sql",  
    "analysis/sql/materialized_views/mv_supplier_monthly_metrics.sql",
    "analysis/sql/materialized_views/mv_sku_price_variance.sql",
    
    # 3. Intelligence Layers (Dependent MVs)
    "analysis/sql/materialized_views/mv_supplier_base.sql",
    "analysis/sql/materialized_views/mv_sku_contract_base.sql",
    
    # 4. Scoring Layers (Dependent on Intelligence)
    "analysis/sql/materialized_views/mv_supplier_scoring.sql",
    "analysis/sql/materialized_views/mv_contract_scoring.sql",
    
    # 5. Final Presentation Layers (Dashboard Ready)
    "analysis/sql/materialized_views/mv_supplier_tiering.sql",
    "analysis/sql/materialized_views/mv_contract_candidates.sql",
    
    # 6. Performance Tuning
    "analysis/sql/indexes.sql"
]

def get_db_engine():
    user = quote_plus(os.getenv("DB_USER", ""))
    pw   = quote_plus(os.getenv("DB_PASS", ""))
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db   = os.getenv("DB_NAME", "procurement")
    url = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    
    # Set isolation_level="AUTOCOMMIT" to allow CREATE MATERIALIZED VIEW commands
    return create_engine(url, isolation_level="AUTOCOMMIT")

def run_sql_file(engine, filepath):
    print(f"📄 Running: {filepath}...")
    
    if not os.path.exists(filepath):
        print(f"❌ Error: File not found: {filepath}")
        return False

    with open(filepath, "r", encoding="utf-8") as f:
        sql_content = f.read()
    
    try:
        # Open a FRESH connection for every file
        with engine.connect() as conn:
            conn.execute(text(sql_content))
        print("   ✅ Success")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False

def main():
    print("🚀 Starting Analytics Database Deployment...")
    engine = get_db_engine()
    
    # Ensure Schema Exists
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS app_analytics;"))
    except Exception as e:
        print(f"⚠️ Schema creation check failed (might already exist): {e}")

    # Run files in order
    for filepath in SQL_EXECUTION_ORDER:
        success = run_sql_file(engine, filepath)
        if not success:
            print("🛑 Aborting deployment due to error.")
            break
        time.sleep(0.1) 

    print("\n✨ Deployment Process Finished.")

if __name__ == "__main__":
    main()
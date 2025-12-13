# step6_db/ingest_from_csv.py
import os, csv, uuid, json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

DB = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT","5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
}

def connect():
    return psycopg2.connect(**DB)

def copy_csv_to_staging(conn, csv_path, table):
    """
    Uses Postgres COPY command for ultra-fast bulk loading into staging.
    """
    if not os.path.exists(csv_path):
        print(f"Skipping {table}: File not found {csv_path}")
        return

    print(f"Loading {csv_path} into {table}...")
    cur = conn.cursor()
    with open(csv_path, 'r', encoding='utf-8') as f:
        # Assumes CSV headers match table columns
        try:
            cur.copy_expert(sql=f"COPY app_core.{table} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'", file=f)
        except Exception as e:
            print(f"Error copying to {table}: {e}")
            conn.rollback()
            raise e
    conn.commit()
    cur.close()

def promote_headers(conn):
    """
    Promote headers from staging to production.
    Safe: Uses ON CONFLICT DO NOTHING (Headers rarely change).
    """
    print("Promoting Headers...")
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO app_core.purchase_order_headers
      SELECT * FROM (
        SELECT purchase_order_id, order_date, buyer_company_name, buyer_email,
               supplier_company_name, supplier_id, subtotal, tax, grand_amount,
               currency, status, cdate, _raw_json
        FROM app_core.staging_headers_tmp
      ) s
      ON CONFLICT (purchase_order_id, order_date) DO NOTHING;
    """)
    conn.commit()
    print(f"Headers processed (Duplicates ignored).")
    cur.close()

def promote_items(conn):
    """
    Promote items from staging to production.
    CRITICAL SAFETY FIX: Uses WHERE NOT EXISTS to prevent duplicates.
    This allows re-running the script safely.
    """
    print("Promoting Items safely (Checking for duplicates)...")
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO app_core.purchase_order_items (
            purchase_order_id,
            purchase_order_no,
            item_id,
            description,
            quantity,
            unit_of_measure,
            unit_price,
            total,
            currency,
            order_date,
            cdate,
            supplier_id,
            plant,
            material_group,
            product_id,
            _raw_json
        )
        SELECT 
            s.purchase_order_id,
            s.purchase_order_no,
            s.item_id,
            s.description,
            s.quantity,
            s.unit_of_measure,
            s.unit_price,
            s.total,
            s.currency,
            s.order_date,
            s.cdate,
            s.supplier_id,
            s.plant,
            s.material_group,
            s.product_id,
            s._raw_json
        FROM app_core.staging_items_tmp s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM app_core.purchase_order_items p 
            WHERE p.purchase_order_id = s.purchase_order_id 
              AND p.purchase_order_no = s.purchase_order_no
        );
    """)
    conn.commit()
    print(f"Items inserted: {cur.rowcount}")
    cur.close()

def clear_staging(conn):
    print("Cleaning up staging tables...")
    cur = conn.cursor()
    cur.execute("TRUNCATE app_core.staging_headers_tmp, app_core.staging_items_tmp;")
    conn.commit()
    cur.close()

def main(headers_csv, items_csv):
    try:
        conn = connect()
        
        # 1. Load CSVs to Staging
        copy_csv_to_staging(conn, headers_csv, 'staging_headers_tmp')
        copy_csv_to_staging(conn, items_csv, 'staging_items_tmp')
        
        # 2. Promote to Production
        promote_headers(conn)
        promote_items(conn)
        
        # 3. Cleanup
        clear_staging(conn)
        
        conn.close()
        print("✅ Ingest complete.")
        
    except Exception as e:
        print(f"❌ Ingestion Failed: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python ingest_from_csv.py <headers.csv> <items.csv>")
        sys.exit(1)
        
    headers_csv = sys.argv[1]
    items_csv = sys.argv[2]
    main(headers_csv, items_csv)
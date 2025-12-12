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
    cur = conn.cursor()
    with open(csv_path, 'r', encoding='utf-8') as f:
        # assume CSV has header matching column names
        cur.copy_expert(sql=f"COPY app_core.{table} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'", file=f)
    conn.commit()
    cur.close()

def promote_headers(conn):
    cur = conn.cursor()
    # For each row in staging_headers_tmp call insert_header_if_not_exists
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
    cur.close()

def promote_items(conn):
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
        FROM app_core.staging_items_tmp;
    """)
    conn.commit()
    cur.close()



def clear_staging(conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE app_core.staging_headers_tmp, app_core.staging_items_tmp;")
    conn.commit()
    cur.close()

def main(headers_csv, items_csv):
    conn = connect()
    copy_csv_to_staging(conn, headers_csv, 'staging_headers_tmp')
    copy_csv_to_staging(conn, items_csv, 'staging_items_tmp')
    promote_headers(conn)
    promote_items(conn)
    clear_staging(conn)
    conn.close()
    print("Ingest complete.")

if __name__ == "__main__":
    import sys
    headers_csv = sys.argv[1]
    items_csv = sys.argv[2]
    main(headers_csv, items_csv)

import json
import os
import math
import re
import logging
from datetime import datetime, timezone
from dateutil import parser
import psycopg2
from psycopg2.extras import execute_batch

# Import your DB connection utility
# (Assuming this exists based on your run_historical_extract.py imports)
from extract.common.db_utils import get_db_connection

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
TOTAL_TOLERANCE = 0.01

# --- HELPER FUNCTIONS (From Step 4) ---
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_sap_date(val):
    if not val: return None
    val_str = str(val).strip()
    if val_str == "": return None
    match = re.search(r"\/Date\((\d+)\)\/", val_str)
    if match:
        try:
            return datetime.fromtimestamp(int(match.group(1)) / 1000.0, tz=timezone.utc).isoformat()
        except: return None
    try:
        return parser.parse(val_str).isoformat()
    except: return None

def clean_numeric(val):
    if val is None or val == "": return None
    try: return float(str(val).replace(",", ""))
    except: return None

def safe_json_dump(obj):
    if not obj: return "{}"
    return json.dumps(obj, ensure_ascii=False).replace("\n", " ").replace("\r", "")

# --- CLEANING LOGIC (From Step 4) ---
def clean_row(row):
    # Ensure header/item json exists
    if "_header_json" not in row: row["_header_json"] = {}
    if "_item_json" not in row: row["_item_json"] = row

    # Validation
    po_no = row.get("purchase_order_no")
    if po_no in (None, "", " "): return None, "missing_item_no"

    # Date Parsing
    row["order_date_iso"] = parse_sap_date(row.get("order_date"))
    row["cdate_iso"]      = parse_sap_date(row.get("cdate"))

    # Numeric Cleaning
    qty   = clean_numeric(row.get("quantity") or row.get("quanity"))
    price = clean_numeric(row.get("unit_price"))
    total = clean_numeric(row.get("total"))

    row["_quantity_float"]   = qty if qty is not None else 0.0
    row["_unit_price_float"] = price if price is not None else 0.0
    row["_total_float"]      = total if total is not None else 0.0

    # Logic Check (Quantity * Price == Total)
    if qty and price and total:
        calc = qty * price
        if not math.isclose(calc, total, abs_tol=TOTAL_TOLERANCE):
            row["_total_mismatch"] = True
    
    # Fill Nulls
    for field in ["plant", "material_group", "product_id"]:
        if field not in row: row[field] = None

    row["item_id"] = str(row["item_id"]).strip() if row.get("item_id") else None
    row["_cleaned_at"] = now_iso()
    
    return row, "success"

# --- MAIN PIPELINE FUNCTION ---
def process_files(file_list):
    """
    Reads raw JSON files, Cleans them, and Inserts into DB.
    Args:
        file_list (list): List of file paths to process.
    """
    logger.info(f"⚙️ Starting Transformation for {len(file_list)} files...")
    
    cleaned_headers = {}
    cleaned_items = []
    
    # 1. READ & CLEAN
    for file_path in file_list:
        if not os.path.exists(file_path):
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            # Depending on SAP format, it might be a JSON list or JSONL
            # Assuming JSON list based on previous fetch code:
            try:
                data = json.load(f) # If file contains [{}, {}]
            except json.JSONDecodeError:
                # Fallback for JSONL (Line by line)
                f.seek(0)
                data = [json.loads(line) for line in f if line.strip()]

            # Handle List vs Single Object
            if isinstance(data, dict): data = [data]
            
            for raw_row in data:
                clean_data, status = clean_row(raw_row)
                
                if status == "success":
                    # --- PREPARE HEADER (Upsert Logic) ---
                    po_id = clean_data.get("purchase_order_id")
                    if po_id and po_id not in cleaned_headers:
                        hdr = clean_data.get("_header_json", {})
                        cleaned_headers[po_id] = (
                            po_id,
                            clean_data.get("order_date_iso"),
                            hdr.get("buyer_company_name"),
                            hdr.get("buyer_email"),
                            hdr.get("supplier_company_name"),
                            hdr.get("supplier_id"),
                            clean_numeric(hdr.get("Subtotal")),
                            clean_numeric(hdr.get("tax")),
                            clean_numeric(hdr.get("grand_amount")),
                            hdr.get("currency"),
                            hdr.get("status"),
                            clean_data.get("cdate_iso"),
                            safe_json_dump(hdr) # _raw_json
                        )

                    # --- PREPARE ITEM ---
                    item_tuple = (
                        clean_data.get("purchase_order_id"),
                        clean_data.get("purchase_order_no"),
                        clean_data.get("item_id"),
                        clean_data.get("description"),
                        clean_data.get("_quantity_float"),
                        clean_data.get("unit_of_measure"),
                        clean_data.get("_unit_price_float"),
                        clean_data.get("_total_float"),
                        clean_data.get("currency"),
                        clean_data.get("order_date_iso"),
                        clean_data.get("cdate_iso"),
                        clean_data.get("plant"),
                        clean_data.get("material_group"),
                        clean_data.get("product_id"),
                        safe_json_dump(clean_data.get("_item_json")) # _raw_json
                    )
                    cleaned_items.append(item_tuple)

    logger.info(f"✅ Data Cleaned. Headers: {len(cleaned_headers)}, Items: {len(cleaned_items)}")
    
    if not cleaned_items:
        logger.warning("⚠️ No valid items found after cleaning. Skipping DB insert.")
        return True

    # 2. INSERT INTO DATABASE
    insert_to_db(list(cleaned_headers.values()), cleaned_items)
    return True

def insert_to_db(headers, items):
    """
    Inserts cleaned data into PostgreSQL.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # SQL for HEADERS (Upsert: If ID exists, do nothing or update)
        header_sql = """
            INSERT INTO app_core.po_headers (
                purchase_order_id, order_date, buyer_company_name, buyer_email,
                supplier_company_name, supplier_id, subtotal, tax, grand_amount,
                currency, status, created_date, raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (purchase_order_id) DO NOTHING;
        """

        # SQL for ITEMS
        item_sql = """
            INSERT INTO app_core.po_items (
                purchase_order_id, purchase_order_no, item_id, description,
                quantity, unit_of_measure, unit_price, total_amount, currency,
                order_date, created_date, plant, material_group, product_id, raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (purchase_order_id, item_id) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                total_amount = EXCLUDED.total_amount,
                unit_price = EXCLUDED.unit_price;
        """

        # Execute Batch Inserts
        logger.info("⏳ Inserting Headers...")
        execute_batch(cur, header_sql, headers)
        
        logger.info("⏳ Inserting Items...")
        execute_batch(cur, item_sql, items)

        conn.commit()
        logger.info("🎉 Database Insert Successful!")

    except Exception as e:
        logger.error(f"❌ Database Insert Failed: {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if conn: conn.close()
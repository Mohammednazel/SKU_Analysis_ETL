# extract/transform/clean_po_data.py
import json
import os
import math
import re
import logging
from datetime import datetime, timezone
from dateutil import parser
import psycopg2
from psycopg2.extras import execute_batch

# Import DB Utilities and Config
from extract.common.db_utils import get_db_connection
from extract.sap.extract_config import REAL_PO_THRESHOLD

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
TOTAL_TOLERANCE = 0.01

# --- HELPER FUNCTIONS ---
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

# --- FLATTENING LOGIC ---
def flatten_raw_data(raw_headers_list):
    """
    Takes a list of Raw SAP Header objects (nested JSON).
    Returns a list of FLATTENED item rows (Dictionary).
    """
    flattened_rows = []
    
    for hdr in raw_headers_list:
        po_id = hdr.get("purchase_order_id")
        
        try:
            if int(po_id) < REAL_PO_THRESHOLD:
                continue
        except:
            continue

        items = []
        if "to_items" in hdr and isinstance(hdr["to_items"], dict):
            items = hdr["to_items"].get("results", [])
        elif "to_items" in hdr and isinstance(hdr["to_items"], list):
            items = hdr["to_items"]

        if not items:
            flattened_rows.append({
                "purchase_order_id": po_id,
                "purchase_order_no": None,
                "item_id": None,
                "description": None,
                "quantity": None,
                "unit_of_measure": None,
                "unit_price": None,
                "total": None,
                "currency": hdr.get("currency"),
                "order_date": hdr.get("order_date"),
                "cdate": hdr.get("cdate"),
                "_header_json": hdr,
                "_item_json": None
            })
            continue

        for item in items:
            flattened_rows.append({
                "purchase_order_id": po_id,
                "purchase_order_no": item.get("purchase_order_no"),
                "item_id": item.get("item_id"),
                "description": item.get("description"),
                "quantity": item.get("quanity") or item.get("quantity"),
                "unit_of_measure": item.get("unit_of_measure"),
                "unit_price": item.get("unit_price"),
                "total": item.get("total"),
                "currency": item.get("currency") or hdr.get("currency"),
                "order_date": hdr.get("order_date"),
                "cdate": hdr.get("cdate"),
                "_header_json": hdr,
                "_item_json": item
            })
            
    return flattened_rows

# --- CLEANING LOGIC ---
def clean_row(row):
    if "_header_json" not in row: row["_header_json"] = {}
    
    row["order_date_iso"] = parse_sap_date(row.get("order_date"))
    row["cdate_iso"]      = parse_sap_date(row.get("cdate"))

    qty   = clean_numeric(row.get("quantity"))
    price = clean_numeric(row.get("unit_price"))
    total = clean_numeric(row.get("total"))

    row["_quantity_float"]   = qty if qty is not None else 0.0
    row["_unit_price_float"] = price if price is not None else 0.0
    row["_total_float"]      = total if total is not None else 0.0

    if qty and price and total:
        calc = qty * price
        if not math.isclose(calc, total, abs_tol=TOTAL_TOLERANCE):
            row["_total_mismatch"] = True
    
    for field in ["plant", "material_group", "product_id"]:
        if field not in row: row[field] = None

    row["item_id"] = str(row["item_id"]).strip() if row.get("item_id") else None
    
    return row, "success"

# --- MAIN PIPELINE FUNCTION ---
def process_files(file_list):
    logger.info(f"⚙️ Starting Transformation for {len(file_list)} files...")
    
    cleaned_headers = {}
    cleaned_items = []
    
    for file_path in file_list:
        if not os.path.exists(file_path):
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                raw_headers = json.load(f)
                if isinstance(raw_headers, dict): raw_headers = [raw_headers]
                
                flat_data = flatten_raw_data(raw_headers)
                
                for flat_row in flat_data:
                    clean_data, status = clean_row(flat_row)
                    
                    if status == "success":
                        po_id = clean_data.get("purchase_order_id")
                        # Extract Header Info
                        hdr = clean_data.get("_header_json", {})
                        
                        # --- PREPARE HEADER ---
                        # Tuple MUST match SQL columns order
                        if po_id and po_id not in cleaned_headers:
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
                        # Tuple MUST match SQL columns order
                        if clean_data.get("item_id"):
                            item_tuple = (
                                clean_data.get("purchase_order_id"),
                                clean_data.get("purchase_order_no"),
                                clean_data.get("item_id"),
                                clean_data.get("description"),
                                clean_data.get("_quantity_float"),
                                clean_data.get("unit_of_measure"),
                                clean_data.get("_unit_price_float"),
                                clean_data.get("_total_float"), # total
                                clean_data.get("currency"),
                                clean_data.get("order_date_iso"),
                                clean_data.get("cdate_iso"),
                                hdr.get("supplier_id"), # supplier_id (New!)
                                clean_data.get("plant"),
                                clean_data.get("material_group"),
                                clean_data.get("product_id"),
                                safe_json_dump(clean_data.get("_item_json")) # _raw_json
                            )
                            cleaned_items.append(item_tuple)
                            
            except json.JSONDecodeError:
                logger.error(f"❌ Failed to decode JSON in {file_path}")
                continue

    logger.info(f"✅ Data Processed. Headers: {len(cleaned_headers)}, Items: {len(cleaned_items)}")
    
    if not cleaned_items and not cleaned_headers:
        logger.warning("⚠️ No valid data found. Skipping DB insert.")
        return True

    insert_to_db(list(cleaned_headers.values()), cleaned_items)
    return True

def insert_to_db(headers, items):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # --- HEADERS: Using Composite Key (purchase_order_id, order_date) ---
        header_sql = """
            INSERT INTO app_core.purchase_order_headers (
                purchase_order_id, order_date, buyer_company_name, buyer_email,
                supplier_company_name, supplier_id, subtotal, tax, grand_amount,
                currency, status, cdate, _raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (purchase_order_id, order_date) DO NOTHING;
        """
        if headers:
            logger.info(f"⏳ Inserting {len(headers)} Headers...")
            execute_batch(cur, header_sql, headers)

        # --- ITEMS: Insert Only (No Conflict Clause for Partitioned Table without PK) ---
        # Fixed Columns: total, cdate, supplier_id, _raw_json
        item_sql = """
            INSERT INTO app_core.purchase_order_items (
                purchase_order_id, purchase_order_no, item_id, description,
                quantity, unit_of_measure, unit_price, total, currency,
                order_date, cdate, supplier_id, plant, material_group, product_id, _raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        if items:
            logger.info(f"⏳ Inserting {len(items)} Items...")
            execute_batch(cur, item_sql, items)

        conn.commit()
        logger.info("🎉 Database Insert Successful!")

    except Exception as e:
        logger.error(f"❌ Database Insert Failed: {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if conn: conn.close()
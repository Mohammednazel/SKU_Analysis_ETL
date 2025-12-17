# data_cleaning_dev/cleaning_step5_5_generate_step6_ready_csvs.py
import os
import json
import csv
import argparse
from datetime import datetime, timezone

# Default Input
DEFAULT_INPUT = "outputs/cleaned_items.jsonl"

STAGING_DIR = "staging"
AUDIT_DIR = "audit"
os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)

HEADERS_CSV = os.path.join(STAGING_DIR, "step6_headers.csv")
ITEMS_CSV   = os.path.join(STAGING_DIR, "step6_items.csv")
BAD_DATA_CSV = os.path.join(AUDIT_DIR, "dropped_missing_date.csv")

HEADER_FIELDS = [
    "purchase_order_id", "order_date", "buyer_company_name", "buyer_email",
    "supplier_company_name", "supplier_id", "subtotal", "tax", "grand_amount",
    "currency", "status", "cdate", "_raw_json"
]
ITEM_FIELDS = [
    "purchase_order_id", "purchase_order_no", "item_id", "description",
    "quantity", "unit_of_measure", "unit_price", "total", "currency",
    "order_date", "cdate", "supplier_id", "plant", "material_group",
    "product_id", "_raw_json"
]

def clean_numeric(val):
    if val is None or val == "": return None
    try: return float(str(val).replace(",", ""))
    except: return None

def safe_json_dump(obj):
    if not obj: return "{}"
    return json.dumps(obj, ensure_ascii=False).replace("\n", " ").replace("\r", "")

def run():
    # --- NEW: Argument Parsing ---
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input Cleaned JSONL file")
    args = parser.parse_args()
    input_file = args.input

    if not os.path.exists(input_file):
        print(f"❌ Input file missing: {input_file}")
        return

    print(f"🚀 Generating CSVs from {input_file}...")
    
    unique_headers = {}
    item_count = 0
    dropped_count = 0
    
    with open(input_file, "r", encoding="utf-8") as fin, \
         open(ITEMS_CSV, "w", newline="", encoding="utf-8") as fitems, \
         open(BAD_DATA_CSV, "w", newline="", encoding="utf-8") as fbad:
        
        writer_items = csv.DictWriter(fitems, fieldnames=ITEM_FIELDS, quoting=csv.QUOTE_ALL)
        writer_items.writeheader()
        writer_bad = csv.writer(fbad)
        writer_bad.writerow(["purchase_order_id", "reason", "raw_json"])
        
        for line in fin:
            if not line.strip(): continue
            try:
                row = json.loads(line)
                o_date = row.get("order_date_iso")
                if not o_date:
                    dropped_count += 1
                    writer_bad.writerow([row.get("purchase_order_id"), "missing_order_date", line[:100]])
                    continue

                po_id = row.get("purchase_order_id")
                hdr_json = row.get("_header_json")
                
                if po_id and hdr_json and po_id not in unique_headers:
                    unique_headers[po_id] = {
                        "purchase_order_id": po_id,
                        "order_date": o_date,
                        "buyer_company_name": hdr_json.get("buyer_company_name"),
                        "buyer_email": hdr_json.get("buyer_email"),
                        "supplier_company_name": hdr_json.get("supplier_company_name"),
                        "supplier_id": hdr_json.get("supplier_id"),
                        "subtotal": clean_numeric(hdr_json.get("Subtotal")),
                        "tax": clean_numeric(hdr_json.get("tax")),
                        "grand_amount": clean_numeric(hdr_json.get("grand_amount")),
                        "currency": hdr_json.get("currency"),
                        "status": hdr_json.get("status"),
                        "cdate": row.get("cdate_iso"),
                        "_raw_json": safe_json_dump(hdr_json)
                    }

                raw_item_obj = row.get("_item_json") or row
                item_out = {
                    "purchase_order_id": po_id,
                    "purchase_order_no": row.get("purchase_order_no"),
                    "item_id": row.get("item_id"),
                    "description": row.get("description"),
                    "quantity": row.get("_quantity_float"),
                    "unit_of_measure": row.get("unit_of_measure"),
                    "unit_price": row.get("_unit_price_float"),
                    "total": row.get("_total_float"),
                    "currency": row.get("currency"),
                    "order_date": o_date,
                    "cdate": row.get("cdate_iso"),
                    "supplier_id": hdr_json.get("supplier_id") if hdr_json else None,
                    "plant": row.get("plant"),
                    "material_group": row.get("material_group"),
                    "product_id": row.get("product_id"),
                    "_raw_json": safe_json_dump(raw_item_obj)
                }
                writer_items.writerow(item_out)
                item_count += 1
            except json.JSONDecodeError: pass

    with open(HEADERS_CSV, "w", newline="", encoding="utf-8") as fheaders:
        writer_hdr = csv.DictWriter(fheaders, fieldnames=HEADER_FIELDS, quoting=csv.QUOTE_ALL)
        writer_hdr.writeheader()
        for h_row in unique_headers.values():
            writer_hdr.writerow(h_row)

    print(f"✅ Done! Items: {item_count} | Headers: {len(unique_headers)}")

if __name__ == "__main__":
    run()
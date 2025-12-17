# data_cleaning_dev/cleaning_step4_date_numeric.py
import json
import os
import math
import re
import argparse
from datetime import datetime, timezone
from dateutil import parser

# Default paths (Fallbacks)
DEFAULT_INPUT  = "extract/outputs/flattened/historical_flattened.jsonl"
DEFAULT_OUTPUT = "outputs/cleaned_items.jsonl"

AUDIT_DIR = "audit"
os.makedirs(AUDIT_DIR, exist_ok=True)
MISSING_ITEMNO_CSV = os.path.join(AUDIT_DIR, "quarantine_missing_itemno.csv")
TOTAL_TOLERANCE = 0.01

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

def clean_row(row):
    if "_header_json" not in row: row["_header_json"] = {}
    if "_item_json" not in row: row["_item_json"] = row

    po_no = row.get("purchase_order_no")
    if po_no in (None, "", " "): return None, "missing_item_no"

    row["order_date_iso"] = parse_sap_date(row.get("order_date"))
    row["cdate_iso"]      = parse_sap_date(row.get("cdate"))

    def to_float(val):
        if val is None or val == "": return None
        try: return float(str(val).replace(",", ""))
        except: return None

    qty   = to_float(row.get("quantity") or row.get("quanity"))
    price = to_float(row.get("unit_price"))
    total = to_float(row.get("total"))

    row["_quantity_float"]   = qty if qty is not None else 0.0
    row["_unit_price_float"] = price if price is not None else 0.0
    row["_total_float"]      = total if total is not None else 0.0

    if qty and price and total:
        calc = qty * price
        if not math.isclose(calc, total, abs_tol=TOTAL_TOLERANCE):
            row["_total_mismatch"] = True
    
    for field in ["plant", "material_group", "product_id"]:
        if field not in row: row[field] = None

    if row.get("item_id"):
        row["item_id"] = str(row["item_id"]).strip()
    else:
        row["item_id"] = None

    row["_cleaned_at"] = now_iso()
    return row, "success"

def main():
    # --- NEW: Argument Parsing ---
    parser = argparse.ArgumentParser(description="Step 4: Clean JSONL Data")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input raw JSONL file")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output cleaned JSONL file")
    args = parser.parse_args()

    input_file = args.input
    output_file = args.output

    if not os.path.exists(input_file):
        print(f"❌ Error: Input file not found: {input_file}")
        return

    print(f"🚀 Starting Cleaning...")
    print(f"   Input:  {input_file}")
    print(f"   Output: {output_file}")
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    stats = {"read": 0, "cleaned": 0, "quarantined_missing_no": 0}
    
    with open(input_file, 'r', encoding='utf-8') as fin, \
         open(output_file, 'w', encoding='utf-8') as fout, \
         open(MISSING_ITEMNO_CSV, 'w', encoding='utf-8') as fquar:

        fquar.write("purchase_order_id,description,reason\n")

        for line in fin:
            if not line.strip(): continue
            stats["read"] += 1
            try:
                raw_row = json.loads(line)
                cleaned_row, status = clean_row(raw_row)
                if status == "success":
                    fout.write(json.dumps(cleaned_row, ensure_ascii=False) + "\n")
                    stats["cleaned"] += 1
                elif status == "missing_item_no":
                    stats["quarantined_missing_no"] += 1
                    fquar.write(f"{raw_row.get('purchase_order_id','')},missing_item_no\n")
            except json.JSONDecodeError: pass
            
            if stats["read"] % 50000 == 0: print(f"   ...processed {stats['read']} rows")

    print(f"✅ Done! Cleaned: {stats['cleaned']} | Output: {output_file}")

if __name__ == "__main__":
    main()
# extract/sap/flatten_pages_streaming.py
import json
from extract.sap.extract_config import FLAT_DIR, REAL_PO_THRESHOLD

def flatten_page_file(input_file, output_file):
    """Reads one page file, flattens items, appends to final JSON file."""
    with open(input_file, "r", encoding="utf-8") as f:
        headers = json.load(f)

    flattened = []

    for hdr in headers:
        po = hdr.get("purchase_order_id")
        try:
            if int(po) < REAL_PO_THRESHOLD:
                continue
        except:
            continue

        items = []
        if "to_items" in hdr:
            items = hdr["to_items"].get("results", [])

        if not items:
            flattened.append({
                "purchase_order_id": po,
                "purchase_order_no": None,
                "item_id": None,
                "description": None,
                "order_date": hdr.get("order_date"),
                "cdate": hdr.get("cdate"),
                "_header_json": hdr
            })
            continue

        for item in items:
            flattened.append({
                "purchase_order_id": po,
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

    # append to output file
    with open(output_file, "a", encoding="utf-8") as f:
        for row in flattened:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

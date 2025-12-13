# extract/sap/flatten_po.py

import json
from collections import defaultdict
from extract.sap.extract_config import REAL_PO_THRESHOLD, FLAT_DIR

def parse_items(headers):
    flattened = []
    po_headers = defaultdict(list)

    for h in headers:
        po = h.get("purchase_order_id")
        po_headers[po].append(h)

        to_items = h.get("to_items", {})
        items = []

        if isinstance(to_items, dict):
            items = to_items.get("results") or []
        elif isinstance(to_items, list):
            items = to_items

        # If no items → still record header
        if not items:
            flattened.append(make_flat_row(h, None))
            continue

        # Flatten each item row
        for it in items:
            flattened.append(make_flat_row(h, it))

    return flattened, po_headers


def make_flat_row(header, item):
    def gs(x): return "" if x is None else str(x)

    return {
        "purchase_order_id": gs(header.get("purchase_order_id")),
        "purchase_order_no": gs(item.get("purchase_order_no") if item else None),
        "item_id": gs(item.get("item_id") if item else None),
        "description": gs(item.get("description") if item else None),
        "quantity": gs(item.get("quanity") if item else None),
        "unit_of_measure": gs(item.get("unit_of_measure") if item else None),
        "unit_price": gs(item.get("unit_price") if item else None),
        "total": gs(item.get("total") if item else None),
        "currency": header.get("currency"),
        "order_date": header.get("order_date"),
        "cdate": header.get("cdate"),
        "supplier_company_name": header.get("supplier_company_name"),
        "grand_amount": header.get("grand_amount"),
        "_header_json": header,
        "_item_json": item
    }


def save_flatten(flattened, label):
    out_path = f"{FLAT_DIR}/{label}_flattened.json"
    json.dump(flattened, open(out_path, "w"), indent=2, ensure_ascii=False)
    return out_path


def filter_real_pos(flattened):
    real = []
    fake = []
    for r in flattened:
        try:
            if int(r["purchase_order_id"]) >= REAL_PO_THRESHOLD:
                real.append(r)
            else:
                fake.append(r)
        except:
            fake.append(r)

    return real, fake

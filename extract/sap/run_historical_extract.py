# extract/sap/run_historical_extract.py
import os
from extract.sap.fetch_po_pages import fetch_and_save_pages
from extract.sap.flatten_pages_streaming import flatten_page_file
from extract.sap.extract_config import FLAT_DIR

FROM = "2024-01-01T00:00:00"
TO   = "2024-02-07T23:59:59"

OUTPUT_FILE = f"{FLAT_DIR}/historical_flattened.jsonl"

def main():
    print("\n=== HISTORICAL EXTRACTION STARTED ===")

    # remove old file
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # Step 1 — streaming fetch
    files = fetch_and_save_pages(FROM, TO, label="historical")

    # Step 2 — streaming flatten
    for f in files:
        flatten_page_file(f, OUTPUT_FILE)

    print("\n=== HISTORICAL EXTRACTION COMPLETE ===")

if __name__ == "__main__":
    main()

# extract/sap/run_daily_extract.py
import os
from datetime import datetime, timedelta, timezone
from extract.sap.fetch_po_pages import fetch_and_save_pages
from extract.sap.flatten_pages_streaming import flatten_page_file
from extract.sap.extract_config import FLAT_DIR

def main():
    # 1. Calculate Yesterday dynamically
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)

    # 2. Define 24-hour window
    FROM = f"{yesterday}T00:00:00"
    TO   = f"{yesterday}T23:59:59"

    # 3. Define Output Filename (Daily specific)
    label = f"daily_{yesterday}"
    output_file = f"{FLAT_DIR}/{label}.jsonl"

    # Cleanup previous run of same day if exists
    if os.path.exists(output_file):
        os.remove(output_file)

    print(f"\n=== DAILY EXTRACT START: {yesterday} ===")
    
    # 4. Fetch & Flatten
    files = fetch_and_save_pages(FROM, TO, label=label)
    
    if not files:
        print(f"No data found for {yesterday}")
        return

    for f in files:
        flatten_page_file(f, output_file)
        # Optional: Delete raw page file immediately to save space?
        # os.remove(f) 

    print(f"\n=== DAILY EXTRACTION COMPLETE ===")
    print(f"Output: {output_file}")

if __name__ == "__main__":
    main()
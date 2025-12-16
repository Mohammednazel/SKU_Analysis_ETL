import os
import argparse
from extract.sap.fetch_po_pages import fetch_and_save_pages
from extract.sap.flatten_pages_streaming import flatten_page_file
# We keep FLAT_DIR as a fallback default, though we likely won't use it in Docker
from extract.sap.extract_config import FLAT_DIR

# Default Date Range (Can also be parameterized if needed later)
FROM = "2024-01-01T00:00:00"

TO   = "2024-03-07T00:00:00"  # Updated to cover full year or relevant history

def main():
    parser = argparse.ArgumentParser(description="Run Historical Extraction")
    parser.add_argument("--output", help="Path to save the flattened JSONL file", required=False)
    args = parser.parse_args()

    # logic: If --output is provided, use it. Otherwise, fallback to default.
    if args.output:
        output_file = args.output
    else:
        output_file = f"{FLAT_DIR}/historical_flattened.jsonl"

    print(f"\n=== HISTORICAL EXTRACTION STARTED ===")
    print(f"📅 Date Range: {FROM} to {TO}")
    print(f"📂 Output File: {output_file}")

    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Remove old file if it exists to start fresh
    if os.path.exists(output_file):
        os.remove(output_file)

    # Step 1 — streaming fetch (This saves raw JSON pages to disk/temp)
    # Note: fetch_and_save_pages writes to its own internal raw directory. 
    # That is fine as long as step 2 can read them.
    files = fetch_and_save_pages(FROM, TO, label="historical")

    # Step 2 — streaming flatten (Reads raw pages, writes to output_file)
    for f in files:
        flatten_page_file(f, output_file)

    print("\n=== HISTORICAL EXTRACTION COMPLETE ===")

if __name__ == "__main__":
    main()
# extract/sap/run_historical_extract.py
import logging
from datetime import datetime, timedelta
# Ensure these variables are set to your FULL 2-year range in config
from extract.sap.extract_config import HISTORICAL_START_DATE, HISTORICAL_END_DATE
from extract.sap.fetch_po_pages import fetch_and_save_pages
from extract.transform.clean_po_data import process_files

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def split_date_range(start_str, end_str, interval_days=30):
    """Generates (start, end) tuples for 30-day chunks."""
    # Parse the strings into Date Objects
    start_date = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S")
    end_date = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S")
    
    current = start_date
    while current < end_date:
        next_hop = current + timedelta(days=interval_days)
        if next_hop > end_date:
            next_hop = end_date
        
        # Format back to string required by SAP
        yield (
            current.strftime("%Y-%m-%dT%H:%M:%S"),
            next_hop.strftime("%Y-%m-%dT%H:%M:%S")
        )
        current = next_hop

def main():
    logger.info(f"🚀 Starting Smart Historical Load: {HISTORICAL_START_DATE} to {HISTORICAL_END_DATE}")
    
    # 1. Generate the list of monthly chunks
    chunks = list(split_date_range(HISTORICAL_START_DATE, HISTORICAL_END_DATE))
    logger.info(f"📅 Split workload into {len(chunks)} monthly batches.")

    total_files = []

    # 2. Loop through each chunk
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        batch_num = i + 1
        logger.info(f"🔄 Batch {batch_num}/{len(chunks)}: Fetching {chunk_start} -> {chunk_end}...")
        
        try:
            # Fetch data for JUST this month
            # We use a unique label (hist_batch_1, hist_batch_2) to avoid filename collisions
            batch_files = fetch_and_save_pages(chunk_start, chunk_end, label=f"hist_batch_{batch_num}")
            
            if batch_files:
                logger.info(f"✅ Batch {batch_num} Success: Saved {len(batch_files)} files.")
                total_files.extend(batch_files)
            else:
                logger.warning(f"⚠️ Batch {batch_num} returned no data (might be expected).")

        except Exception as e:
            # If one batch totally fails (e.g. auth error), log it and try the next month
            logger.error(f"❌ Batch {batch_num} Failed: {e}. Continuing to next batch...")

    logger.info(f"🏁 All extraction batches complete. Total files collected: {len(total_files)}")

    # 3. Process all downloaded files (ETL)
    if total_files:
        logger.info("⚙️ Transforming and Loading data into DB...")
        process_files(total_files)
        logger.info("🎉 Historical Load Complete!")
    else:
        logger.error("❌ No files were downloaded in any batch.")

if __name__ == "__main__":
    main()
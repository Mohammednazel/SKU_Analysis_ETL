import logging
import os
from datetime import datetime, timedelta

# Import configuration
from extract.sap.extract_config import HISTORICAL_START_DATE, HISTORICAL_END_DATE
from extract.sap.fetch_po_pages import fetch_and_save_pages

# Import the "Dummy" placeholder we just created (or the real one later)
# This prevents the ModuleNotFoundError you saw earlier
from extract.transform.clean_po_data import process_files

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def split_date_range(start_str, end_str, interval_days=30):
    """
    Helper Function:
    Takes a start and end string (e.g., '2023-01-01', '2025-01-01')
    and yields small 30-day chunks (start, end).
    """
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
    logger.info(f"🚀 Starting Smart Historical Load")
    logger.info(f"📅 Overall Target: {HISTORICAL_START_DATE} to {HISTORICAL_END_DATE}")
    
    # 1. Generate the list of monthly chunks
    chunks = list(split_date_range(HISTORICAL_START_DATE, HISTORICAL_END_DATE))
    logger.info(f"📊 Strategy: Split workload into {len(chunks)} monthly batches.")

    total_files_collected = []

    # 2. Loop through each chunk
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        batch_num = i + 1
        logger.info(f"🔄 Batch {batch_num}/{len(chunks)}: Fetching {chunk_start} -> {chunk_end}...")
        
        try:
            # We add a unique label (hist_batch_X) so files don't overwrite each other
            batch_files = fetch_and_save_pages(chunk_start, chunk_end, label=f"hist_batch_{batch_num}")
            
            if batch_files:
                logger.info(f"✅ Batch {batch_num} Success: Saved {len(batch_files)} files.")
                total_files_collected.extend(batch_files)
            else:
                logger.warning(f"⚠️ Batch {batch_num} returned no data.")

        except Exception as e:
            # If a specific month fails, we log it but CONTINUING to the next month
            logger.error(f"❌ Batch {batch_num} Failed with error: {e}")
            logger.info("⏭️ Moving to next batch...")

    # 3. Process/Transform the files
    # (This calls the placeholder function you created in extract/transform/clean_po_data.py)
    if total_files_collected:
        logger.info(f"🏁 Extraction Complete. Total files: {len(total_files_collected)}")
        logger.info("⚙️ Starting Transformation & Load...")
        
        process_files(total_files_collected)
        
        logger.info("🎉 Historical Job Finished Successfully.")
    else:
        logger.error("❌ No files were downloaded in any batch.")

if __name__ == "__main__":
    main()
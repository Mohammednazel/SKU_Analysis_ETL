# extract/sap/run_historical_extract.py
import os
import sys
import logging
import shutil
from datetime import datetime

# Setup paths
sys.path.append("/app")

# Import Modules
from extract.sap.extract_config import OUTPUT_DIR
from extract.sap.fetch_po_pages import fetch_po_data_range
from extract.transform.clean_po_data import process_files
from extract.common.batch_manager import (
    initialize_batches, get_next_batch, 
    mark_batch_complete, mark_batch_failed
)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Starting Smart Historical Pipeline...")
    
    # 1. Ensure Batch Table Exists & Is Populated
    try:
        initialize_batches()
    except Exception as e:
        logger.error(f"❌ Failed to initialize batches: {e}")
        return

    # 2. Loop through batches until done
    while True:
        batch = get_next_batch()
        
        if not batch:
            logger.info("✅ All historical batches are COMPLETED! Nothing to do.")
            break
            
        b_id = batch["id"]
        start_str = batch["start"].strftime("%Y-%m-%dT%H:%M:%S")
        end_str   = batch["end"].strftime("%Y-%m-%dT%H:%M:%S")
        
        logger.info(f"▶️ Processing Batch #{b_id}: {start_str} -> {end_str}")
        
        try:
            # A. Clean up raw folder (Don't mix data from previous batches)
            raw_dir = os.path.join(OUTPUT_DIR, "raw")
            if os.path.exists(raw_dir):
                shutil.rmtree(raw_dir)
            os.makedirs(raw_dir, exist_ok=True)
            
            # B. EXTRACT (Download JSONs)
            files = fetch_po_data_range(start_str, end_str, f"batch_{b_id}")
            
            if not files:
                logger.warning(f"⚠️ Batch #{b_id} returned no files. Marking complete as empty.")
                mark_batch_complete(b_id, 0, 0)
                continue
                
            # C. TRANSFORM & LOAD (Insert to DB)
            # Note: process_files now returns a count of inserted items if we modify it slightly, 
            # but for now we trust it works.
            success = process_files(files)
            
            if success:
                # We count files to log progress
                mark_batch_complete(b_id, len(files), 0) # 0 rows for now unless we update return
                logger.info(f"🎉 Batch #{b_id} Completed Successfully.")
            else:
                raise Exception("Transformation returned False")
                
        except Exception as e:
            logger.error(f"❌ Batch #{b_id} Failed: {e}")
            mark_batch_failed(b_id, str(e))
            # We exit on failure to let you investigate, or we could continue?
            # Let's continue to next batch so one failure doesn't stop the whole train.
            continue

if __name__ == "__main__":
    main()
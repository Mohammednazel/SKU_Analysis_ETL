# extract/transform/clean_po_data.py
import logging

logger = logging.getLogger(__name__)

def process_files(file_list):
    """
    Placeholder function.
    Eventually, this will clean the JSON data and insert it into the DB.
    For now, it just acknowledges the files so the job doesn't crash.
    """
    logger.info("--------------------------------------------------")
    logger.info(f"🚧 TRANSFORMATION PLACEHOLDER: Received {len(file_list)} files.")
    logger.info("ℹ️ Skipping detailed processing for this test run.")
    logger.info("--------------------------------------------------")
    return True
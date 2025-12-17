# extract/sap/fetch_po_pages.py
import os
import requests
import json
import logging
import time
from extract.sap.extract_config import (
    SAP_PO_URL, TIMEOUT, RAW_DIR, PAGE_SIZE, MAX_PAGES
)
from extract.sap.token_manager import get_sap_token

logger = logging.getLogger(__name__)

def fetch_po_data_range(start_date_str, end_date_str, batch_label="batch"):
    """
    Fetches PO data using the 'Old Code' strategy: 
    GET request with JSON BODY including DATE FILTERS.
    """
    saved_files = []
    token = get_sap_token()
    
    # Format dates to match SAP requirements (usually YYYY-MM-DD)
    # Removing 'T00:00:00' if present, as SAP often prefers simple dates
    sap_start_date = start_date_str.split("T")[0]
    sap_end_date = end_date_str.split("T")[0]

    logger.info(f"🔄 Fetching data from {sap_start_date} to {sap_end_date}...")

    skiptoken = 0
    
    for page in range(1, MAX_PAGES + 1):
        # 1. Construct the Payload (Restoring your ORIGINAL Logic)
        payload = {
            "expand": "to_items",
            "from_cdate": sap_start_date,  # <--- RESTORED
            "to_cdate": sap_end_date,      # <--- RESTORED
            # Only add skiptoken if it's > 0 (Matching your old code logic)
        }
        if skiptoken > 0:
            payload["skiptoken"] = str(skiptoken)

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        try:
            response = None
            for attempt in range(3):
                try:
                    response = requests.get(
                        SAP_PO_URL, 
                        headers=headers, 
                        json=payload, 
                        timeout=TIMEOUT
                    )
                    
                    if response.status_code == 401:
                        logger.warning("🔄 Token expired. Refreshing...")
                        token = get_sap_token(force_refresh=True)
                        headers["Authorization"] = f"Bearer {token}"
                        continue
                    
                    if response.status_code >= 500:
                        logger.warning(f"⚠️ Server Error {response.status_code}. Retrying ({attempt+1}/3)...")
                        time.sleep(5)
                        continue
                    
                    response.raise_for_status()
                    break 

                except requests.exceptions.RequestException as e:
                    logger.warning(f"⚠️ Network error: {e}. Retrying...")
                    time.sleep(2)
            
            if response is None or response.status_code != 200:
                raise Exception(f"Failed to fetch page {page}.")

            data = response.json()
            
            # 2. Extract Results
            results = []
            if "d" in data:
                d_data = data["d"]
                if isinstance(d_data, list):
                    results = d_data
                elif "results" in d_data:
                    results = d_data["results"]
            
            # 3. Stop if empty
            if not results:
                logger.info(f"✅ No more data at page {page}. Stopping.")
                break

            # 4. Save Raw Data
            # Note: We trust the Server-Side filter now, but we keep the file saving logic
            if results:
                filename = f"{batch_label}_page_{page}.json"
                filepath = os.path.join(RAW_DIR, filename)
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False)
                saved_files.append(filepath)
                logger.info(f"💾 Saved {len(results)} rows to {filename}")

            # 5. Advance Pagination
            skiptoken += PAGE_SIZE
            
            # Safety break
            if len(results) < 5: 
                 logger.info("✅ Result count low, assuming end of data.")
                 break

        except Exception as e:
            logger.error(f"❌ Batch crashed at page {page}: {e}")
            raise e

    return saved_files
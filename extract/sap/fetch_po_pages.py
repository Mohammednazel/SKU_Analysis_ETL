# extract/sap/fetch_po_pages.py
import os
import requests
import json
import logging
import time
from extract.sap.extract_config import (
    SAP_PO_URL, PAGE_SIZE, MAX_PAGES, TIMEOUT, 
    RAW_DIR, OUTPUT_DIR
)
from extract.sap.token_manager import get_sap_token

logger = logging.getLogger(__name__)

def fetch_po_data_range(start_date_str, end_date_str, batch_label="batch"):
    """
    Fetches PO data using SIMPLE requests.get (No Session/Adapter).
    Mimics the original script that worked for Batch 1.
    """
    saved_files = []
    token = get_sap_token()
    
    logger.info(f"🔄 Fetching data from {start_date_str} to {end_date_str}...")

    for page in range(1, MAX_PAGES + 1):
        # 1. Define Params
        params = {
            "$top": PAGE_SIZE,
            "$skip": (page - 1) * PAGE_SIZE,
            "$format": "json"
        }
        
        # 2. Define Headers
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Connection": "close" # Force close to avoid Keep-Alive issues
        }

        success = False
        retries = 3
        
        # 3. Manual Retry Loop (Simple)
        while retries > 0:
            try:
                # Direct requests.get (No Session)
                response = requests.get(SAP_PO_URL, params=params, headers=headers, timeout=TIMEOUT)
                
                if response.status_code == 401:
                    logger.warning("🔄 Token expired. Refreshing...")
                    token = get_sap_token(force_refresh=True)
                    headers["Authorization"] = f"Bearer {token}"
                    continue # Retry loop
                
                if response.status_code == 500:
                    logger.warning(f"⚠️ 500 Error on page {page}. Retrying in 5s...")
                    time.sleep(5)
                    retries -= 1
                    continue

                response.raise_for_status()
                success = True
                break # Exit retry loop
            
            except Exception as e:
                logger.warning(f"⚠️ Request failed: {e}. Retries left: {retries}")
                retries -= 1
                time.sleep(2)

        if not success:
            error_msg = f"❌ Failed to fetch page {page} after retries."
            logger.error(error_msg)
            # IMPORTANT: We raise error to stop the batch so it marks FAILED
            raise Exception(error_msg)
            
        # 4. Process Data
        data = response.json()
        
        results = []
        if isinstance(data, list): results = data
        elif "d" in data:
            d = data["d"]
            if isinstance(d, list): results = d
            elif "results" in d: results = d["results"]
        elif "results" in data: results = data["results"]

        if not results:
            logger.info(f"✅ No more data at page {page}. Stopping.")
            break

        # 5. Filter Data (Client Side)
        filtered_results = []
        for item in results:
            item_date = item.get("order_date") or item.get("cdate") or item.get("created_at") or item.get("CreationDate")
            
            # If date exists, check range. If not, include it to be safe.
            if item_date:
                if start_date_str <= str(item_date) < end_date_str:
                    filtered_results.append(item)
            else:
                filtered_results.append(item)

        # 6. Save
        if filtered_results:
            filename = f"{batch_label}_page_{page}.json"
            filepath = os.path.join(RAW_DIR, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(filtered_results, f, ensure_ascii=False)
            saved_files.append(filepath)
            logger.info(f"💾 Saved {len(filtered_results)} rows to {filename}")

    return saved_files
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
    GET request with JSON BODY.
    """
    saved_files = []
    token = get_sap_token()
    
    logger.info(f"🔄 Fetching data from {start_date_str} to {end_date_str} using JSON Body strategy...")

    # We use a numeric skiptoken because that is how your old code worked
    skiptoken = 0
    
    for page in range(1, MAX_PAGES + 1):
        # 1. Construct the JSON Body (Mimicking your working Postman/Old Script)
        # Note: We do NOT use URL parameters like $top/$skip. We use the body.
        payload = {
            "expand": "to_items",   # Critical: No "$" prefix
            "skiptoken": str(skiptoken)
        }
        
        # Optional: If your API supports date filters in the body, we could add them.
        # But since your Postman worked without them, we will fetch raw and filter in Python
        # to be 100% safe and avoid 500 errors.

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        try:
            # RETRY LOGIC for 500 Errors
            response = None
            for attempt in range(3):
                try:
                    # CRITICAL: Sending json=payload in a GET request
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
                        logger.warning(f"⚠️ Server Error {response.status_code} on page {page}. Retrying ({attempt+1}/3)...")
                        time.sleep(5)
                        continue
                    
                    # If we get here, it's not a 500 or 401
                    response.raise_for_status()
                    break 

                except requests.exceptions.RequestException as e:
                    logger.warning(f"⚠️ Network error: {e}. Retrying...")
                    time.sleep(2)
            
            # If still failing after retries
            if response is None or response.status_code != 200:
                raise Exception(f"Failed to fetch page {page}. Final status: {response.status_code if response else 'None'}")

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

            # 4. Filter & Save
            filtered_results = []
            for item in results:
                # Flexible date finder
                item_date = item.get("order_date") or item.get("cdate") or item.get("created_at")
                
                if item_date:
                    # String comparison for ISO dates
                    if start_date_str <= str(item_date) < end_date_str:
                        filtered_results.append(item)
                else:
                    # Safe fallback
                    filtered_results.append(item)

            if filtered_results:
                filename = f"{batch_label}_page_{page}.json"
                filepath = os.path.join(RAW_DIR, filename)
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(filtered_results, f, ensure_ascii=False)
                saved_files.append(filepath)
                logger.info(f"💾 Saved {len(filtered_results)} rows to {filename}")

            # 5. Advance Pagination (Old Code Logic)
            # Increment by PAGE_SIZE
            skiptoken += PAGE_SIZE
            
            # Protection against infinite loops if server ignores skiptoken
            if len(results) < 5: # If we got fewer than 5 items, we are likely done
                 logger.info("✅ Result count low, assuming end of data.")
                 break

        except Exception as e:
            logger.error(f"❌ Batch crashed at page {page}: {e}")
            raise e

    return saved_files
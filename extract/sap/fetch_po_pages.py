# extract/sap/fetch_po_pages.py
import os
import requests
import json
import logging
from extract.sap.extract_config import (
    SAP_PO_URL, TIMEOUT, RAW_DIR, PAGE_SIZE, MAX_PAGES
)
from extract.sap.token_manager import get_sap_token

logger = logging.getLogger(__name__)

def fetch_po_data_range(start_date_str, end_date_str, batch_label="batch"):
    """
    Fetches PO data using standard OData parameters.
    CRITICAL FIX: Includes $expand=to_items to prevent Server 500 Errors.
    """
    saved_files = []
    token = get_sap_token()
    
    logger.info(f"🔄 Fetching data from {start_date_str} to {end_date_str}...")

    # We use a session for efficiency
    session = requests.Session()
    
    for page in range(1, MAX_PAGES + 1):
        # Calculate Offset ($skip)
        offset = (page - 1) * PAGE_SIZE

        # Standard OData Parameters
        # This matches your Postman logic but uses standard URL params
        params = {
            "$format": "json",
            "$expand": "to_items",  # <--- PREVENTS 500 ERROR
            "$top": PAGE_SIZE,
            "$skip": offset
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

        try:
            # Simple Retry Logic for 500s
            response = None
            for attempt in range(3):
                response = session.get(SAP_PO_URL, params=params, headers=headers, timeout=TIMEOUT)
                
                if response.status_code == 401:
                    logger.warning("🔄 Token expired. Refreshing...")
                    token = get_sap_token(force_refresh=True)
                    headers["Authorization"] = f"Bearer {token}"
                    continue
                
                if response.status_code >= 500:
                    logger.warning(f"⚠️ Server Error {response.status_code}. Retrying ({attempt+1}/3)...")
                    import time; time.sleep(2)
                    continue
                
                break # Success or 4xx error

            response.raise_for_status()
            data = response.json()
            
            # 1. Extract Results
            results = []
            if "d" in data:
                d_data = data["d"]
                if isinstance(d_data, list):
                    results = d_data
                elif "results" in d_data:
                    results = d_data["results"]
            
            # 2. Stop if empty
            if not results:
                logger.info(f"✅ No more data at page {page}. Stopping.")
                break

            # 3. Filter & Save
            filtered_results = []
            for item in results:
                item_date = item.get("order_date") or item.get("cdate") or item.get("created_at")
                if item_date:
                    if start_date_str <= str(item_date) < end_date_str:
                        filtered_results.append(item)
                else:
                    filtered_results.append(item)

            if filtered_results:
                filename = f"{batch_label}_page_{page}.json"
                filepath = os.path.join(RAW_DIR, filename)
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(filtered_results, f, ensure_ascii=False)
                saved_files.append(filepath)
                logger.info(f"💾 Saved {len(filtered_results)} rows to {filename}")

        except Exception as e:
            logger.error(f"❌ Batch crashed at page {page}: {e}")
            raise e

    return saved_files
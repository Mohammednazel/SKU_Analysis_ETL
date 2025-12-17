# extract/sap/fetch_po_pages.py
import os
import requests
import json
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from extract.sap.extract_config import (
    SAP_PO_URL, PAGE_SIZE, MAX_PAGES, TIMEOUT, 
    RAW_DIR, OUTPUT_DIR
)
from extract.sap.token_manager import get_sap_token

logger = logging.getLogger(__name__)

def create_retry_session(retries=5, backoff_factor=1, status_forcelist=(500, 502, 503, 504)):
    """
    Creates a requests Session with automatic retries for server errors.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_po_data_range(start_date_str, end_date_str, batch_label="batch"):
    """
    Fetches PO data from SAP for a specific date range with RETRIES.
    """
    saved_files = []
    token = get_sap_token()
    
    # Use a session with Retry logic
    session = create_retry_session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    })

    logger.info(f"🔄 Fetching data from {start_date_str} to {end_date_str}...")

    for page in range(1, MAX_PAGES + 1):
        params = {
            "$top": PAGE_SIZE,
            "$skip": (page - 1) * PAGE_SIZE,
            "$format": "json"
        }
        
        try:
            # The session.get will now auto-retry on 500 errors
            response = session.get(SAP_PO_URL, params=params, timeout=TIMEOUT)
            
            # Handle Token Expiry (401 is usually not covered by Retry)
            if response.status_code == 401:
                logger.warning("🔄 Token expired. Refreshing...")
                token = get_sap_token(force_refresh=True)
                session.headers.update({"Authorization": f"Bearer {token}"})
                response = session.get(SAP_PO_URL, params=params, timeout=TIMEOUT)

            response.raise_for_status()
            
            data = response.json()
            
            # Normalize Response
            results = []
            if isinstance(data, list):
                results = data
            elif "d" in data:
                d = data["d"]
                if isinstance(d, list): results = d
                elif "results" in d: results = d["results"]
            elif "results" in data:
                results = data["results"]

            if not results:
                logger.info(f"✅ No more data at page {page}. Stopping.")
                break

            # --- CLIENT-SIDE DATE FILTERING ---
            filtered_results = []
            for item in results:
                # Check various date fields
                item_date = item.get("order_date") or item.get("cdate") or item.get("created_at") or item.get("CreationDate")
                
                # If no date found, we include it to be safe (or you can choose to skip)
                if not item_date:
                    filtered_results.append(item)
                    continue

                # String Comparison for ISO Dates
                if start_date_str <= str(item_date) < end_date_str:
                    filtered_results.append(item)

            # SAVE RAW FILE (Only if filter caught something)
            if filtered_results:
                filename = f"{batch_label}_page_{page}.json"
                filepath = os.path.join(RAW_DIR, filename)
                
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(filtered_results, f, ensure_ascii=False)
                
                saved_files.append(filepath)
                logger.info(f"💾 Saved {len(filtered_results)} rows to {filename}")

        except Exception as e:
            # If we are here, even the retries failed
            logger.error(f"❌ Failed to fetch page {page} after retries: {e}")
            raise e
            
        # Small delay to be nice to the server
        time.sleep(0.2)

    return saved_files
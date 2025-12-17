# extract/sap/fetch_po_pages.py
import os
import requests
import json
import logging
import time
from extract.sap.extract_config import (
    SAP_PO_URL, TIMEOUT, RAW_DIR
)
from extract.sap.token_manager import get_sap_token

logger = logging.getLogger(__name__)

def fetch_po_data_range(start_date_str, end_date_str, batch_label="batch"):
    """
    Fetches PO data using OData Pagination (skiptoken) + Expansion.
    Matches the working Postman configuration.
    """
    saved_files = []
    token = get_sap_token()
    
    logger.info(f"🔄 Fetching data from {start_date_str} to {end_date_str}...")

    # Initial URL (No skiptoken yet)
    # CRITICAL: We MUST include $expand=to_items to prevent server crash
    current_url = SAP_PO_URL
    
    # We use a session for efficiency
    session = requests.Session()
    
    page_count = 1
    has_more = True
    
    # Initial Params
    params = {
        "$format": "json",
        "$expand": "to_items",  # <--- THE MISSING KEY
        "$top": 100             # Fetch 100 at a time
    }

    while has_more:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

        try:
            response = session.get(current_url, params=params, headers=headers, timeout=TIMEOUT)
            
            # Handle Token Refresh
            if response.status_code == 401:
                logger.warning("🔄 Token expired. Refreshing...")
                token = get_sap_token(force_refresh=True)
                continue # Retry same request
            
            # Handle 500 (Retry once or twice, but don't loop forever)
            if response.status_code >= 500:
                logger.error(f"❌ Server Error {response.status_code}: {response.text}")
                # We raise error to trigger the batch retry logic if needed
                response.raise_for_status()

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
            
            # 2. Filter & Save
            filtered_results = []
            for item in results:
                # Flexible date check
                item_date = item.get("order_date") or item.get("cdate") or item.get("created_at")
                if item_date:
                    # String comparison for ISO dates
                    if start_date_str <= str(item_date) < end_date_str:
                        filtered_results.append(item)
                else:
                    # If date missing, keep it to be safe
                    filtered_results.append(item)

            if filtered_results:
                filename = f"{batch_label}_page_{page_count}.json"
                filepath = os.path.join(RAW_DIR, filename)
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(filtered_results, f, ensure_ascii=False)
                saved_files.append(filepath)
                logger.info(f"💾 Saved {len(filtered_results)} rows to {filename}")
            else:
                logger.info(f"ℹ️ Page {page_count} fetched, but 0 rows matched date range.")

            # 3. Handle Pagination (Next Link / skiptoken)
            # SAP OData usually returns a '__next' link in 'd' if there is more data
            next_link = None
            if "d" in data and "__next" in data["d"]:
                next_link = data["d"]["__next"]
            
            if next_link:
                # The next_link usually contains the full URL with skiptoken
                # We update current_url to this new link and CLEAR params 
                # (because the link already has them embedded)
                current_url = next_link
                params = {} # Clear params so we don't double-add them
                page_count += 1
            else:
                logger.info("✅ No '__next' link found. Pagination complete.")
                has_more = False

        except Exception as e:
            logger.error(f"❌ Batch crashed at page {page_count}: {e}")
            raise e

    return saved_files
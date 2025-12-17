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
    Fetches PO data from SAP for a specific date range.
    
    Args:
        start_date_str (str): ISO start date (e.g., '2024-01-01T00:00:00')
        end_date_str (str): ISO end date (e.g., '2024-02-01T00:00:00')
        batch_label (str): Label for file naming (e.g., 'batch_24')
        
    Returns:
        list: List of file paths saved (e.g., ['.../batch_24_page_1.json', ...])
    """
    saved_files = []
    token = get_sap_token()
    
    # OData Filter: Created Date between Start and End
    # Note: Syntax depends on SAP version. Common OData uses $filter=CreationDate ge ... and CreationDate lt ...
    # Adjust 'CreationDate' to your actual SAP field name (e.g. created_at, CDate, etc.)
    # For now, we assume your endpoint supports the 'filter' param or standard OData.
    # If your API is simple pagination without filters, we fetch and filter in memory (less efficient but works).
    
    # --- LOGIC: PAGINATION LOOP ---
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    })

    logger.info(f"🔄 Fetching data from {start_date_str} to {end_date_str}...")

    for page in range(1, MAX_PAGES + 1):
        # Construct Params
        # Note: If your API supports server-side date filtering, add it here.
        # Example: "$filter": f"CreationDate ge datetime'{start_date_str}'..."
        params = {
            "$top": PAGE_SIZE,
            "$skip": (page - 1) * PAGE_SIZE,
            "$format": "json"
        }
        
        # If your specific API uses a custom filter param, add it here:
        # params["start_date"] = start_date_str 
        # params["end_date"] = end_date_str

        try:
            response = session.get(SAP_PO_URL, params=params, timeout=TIMEOUT)
            
            if response.status_code == 401:
                logger.warning("🔄 Token expired. Refreshing...")
                token = get_sap_token(force_refresh=True)
                session.headers.update({"Authorization": f"Bearer {token}"})
                # Retry current page
                response = session.get(SAP_PO_URL, params=params, timeout=TIMEOUT)

            response.raise_for_status()
            
            data = response.json()
            
            # Normalize SAP response structure (handle 'd', 'results', or root list)
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

            # --- CRITICAL: CLIENT-SIDE DATE FILTERING ---
            # If the API doesn't support date params, we must filter here.
            # We assume the field is 'created_date' or 'cdate' or 'order_date'
            filtered_results = []
            for item in results:
                # Try to find the date field
                item_date = item.get("order_date") or item.get("cdate") or item.get("created_at")
                
                # If we can't find a date, we include it to be safe, or log warning
                if not item_date:
                    filtered_results.append(item)
                    continue

                # Simple string comparison works for ISO dates (YYYY-MM-DD...)
                # Check if item_date is within range [start, end)
                if start_date_str <= str(item_date) < end_date_str:
                    filtered_results.append(item)

            # If filtered_results is empty but we are paginating, we keep going
            # because data might not be sorted by date.
            
            # SAVE RAW FILE (Only if we have data)
            if filtered_results:
                filename = f"{batch_label}_page_{page}.json"
                filepath = os.path.join(RAW_DIR, filename)
                
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(filtered_results, f, ensure_ascii=False)
                
                saved_files.append(filepath)
                logger.info(f"💾 Saved {len(filtered_results)} rows to {filename}")
            else:
                # If using Server-Side filtering, empty results usually mean we are done.
                # If Client-Side, we might need to verify if we should stop.
                # For now, we assume simple pagination.
                pass

        except Exception as e:
            logger.error(f"❌ Failed to fetch page {page}: {e}")
            # In a batch system, we might want to retry or stop. 
            # Raising error stops this batch so it can be retried later.
            raise e
            
        time.sleep(0.5) # Rate limit protection

    return saved_files
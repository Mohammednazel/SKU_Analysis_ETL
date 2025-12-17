# extract/sap/fetch_po_pages.py
import json
import time
import os
import requests

# --- FIX: Import the correct function name (get_sap_token) ---
from extract.sap.token_manager import get_sap_token 

from extract.sap.extract_config import (
    NADEC_PO_URL, SAP_CLIENT, PAGE_SIZE,
    MAX_PAGES, TIMEOUT, RAW_DIR
)

def request_page(headers, body, skiptoken):
    if skiptoken > 0:
        body["skiptoken"] = str(skiptoken)

    for attempt in range(5):  # INCREASED RETRIES FROM 3 TO 5
        try:
            resp = requests.get(
                NADEC_PO_URL,
                headers=headers,
                json=body,
                timeout=120  # Keep your increased timeout
            )
            if resp.status_code == 429:
                print(f"[WARN] 429 Too Many Requests. Sleeping...")
                time.sleep(10)
                continue
            
            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            print(f"[WARN] retry {attempt+1}/5 due to {e}")
            time.sleep(5 + attempt * 2) # INCREASED SLEEP TIME

    raise RuntimeError("Page fetch failed after retries")

def fetch_and_save_pages(from_cdate, to_cdate, label="extract"):
    # --- FIX: Use the correct function call ---
    token = get_sap_token()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Cookie": f"sap-usercontext=sap-client={SAP_CLIENT}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # This logic matches your "Old Code" exactly
    body = {"expand": "to_items", "from_cdate": from_cdate, "to_cdate": to_cdate}

    skiptoken = 0
    saved_files = []

    print(f"🔄 Fetching {from_cdate} -> {to_cdate}...")

    for page in range(1, MAX_PAGES + 1):
        try:
            # 🛡️ THE SAFETY NET: Try to get the page
            data = request_page(headers, body, skiptoken)
            
        except RuntimeError as e:
            # If SAP crashes, Log it, STOP extracting, but RETURN what we have
            print(f"⚠️ [PARTIAL SUCCESS] SAP Failed at page {page}: {e}")
            print(f"✅ Stopping this batch, but keeping {len(saved_files)} saved pages.")
            break 

        rows = []
        if "d" in data and "results" in data["d"]:
            rows = data["d"]["results"]
        elif "value" in data:
            rows = data["value"]

        if not rows:
            print(f"[STOP] no more rows at page {page}")
            break

        filename = f"{label}_page_{page}.json"
        filepath = os.path.join(RAW_DIR, filename)
        
        # Ensure directory exists (just in case)
        os.makedirs(RAW_DIR, exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)

        print(f"[SAVED] {filename} ({len(rows)} rows)")
        saved_files.append(filepath)

        skiptoken += PAGE_SIZE
        time.sleep(0.1)

    return saved_files
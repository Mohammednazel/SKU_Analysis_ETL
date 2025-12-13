# extract/sap/fetch_po_pages.py
import json, time, os, requests
from extract.sap.token_manager import fetch_token
from extract.sap.extract_config import (
    NADEC_PO_URL, SAP_CLIENT, PAGE_SIZE,
    MAX_PAGES, TIMEOUT, RAW_DIR
)

def request_page(headers, body, skiptoken):
    if skiptoken > 0:
        body["skiptoken"] = str(skiptoken)

    for attempt in range(3):
        try:
            resp = requests.get(
                NADEC_PO_URL,
                headers=headers,
                json=body,
                timeout=TIMEOUT
            )
            if resp.status_code == 429:
                time.sleep(5)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[WARN] retry due to {e}")
            time.sleep(2 + attempt)
    raise RuntimeError("Page fetch failed after retries")

def fetch_and_save_pages(from_cdate, to_cdate, label="extract"):
    token = fetch_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Cookie": f"sap-usercontext=sap-client={SAP_CLIENT}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    body = {"expand": "to_items", "from_cdate": from_cdate, "to_cdate": to_cdate}

    skiptoken = 0
    saved_files = []

    for page in range(1, MAX_PAGES + 1):
        data = request_page(headers, body, skiptoken)

        rows = []
        if "d" in data and "results" in data["d"]:
            rows = data["d"]["results"]
        elif "value" in data:
            rows = data["value"]

        if not rows:
            print(f"[STOP] no more rows at page {page}")
            break

        filename = f"{RAW_DIR}/{label}_page_{page}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)

        print(f"[SAVED] {filename} ({len(rows)} rows)")
        saved_files.append(filename)

        skiptoken += PAGE_SIZE
        time.sleep(0.1)

    return saved_files

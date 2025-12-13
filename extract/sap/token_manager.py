# extract/sap/token_manager.py

import json, os, time, requests
from extract.sap.extract_config import TOKEN_URL, CLIENT_ID, CLIENT_SECRET, TIMEOUT

TOKEN_CACHE = "extract/.token_cache.json"

def load_cached_token():
    if not os.path.exists(TOKEN_CACHE):
        return None

    try:
        data = json.load(open(TOKEN_CACHE))
        if data.get("expires_at", 0) > time.time():
            return data["token"]
    except:
        return None

    return None


def save_token(token, expires_in):
    data = {
        "token": token,
        "expires_at": time.time() + expires_in - 10
    }
    json.dump(data, open(TOKEN_CACHE, "w"))


def fetch_token():
    cached = load_cached_token()
    if cached:
        print("[TOKEN] Using cached token.")
        return cached

    print("[TOKEN] Fetching new token...")
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        },
        timeout=TIMEOUT
    )
    resp.raise_for_status()
    data = resp.json()

    token = data["access_token"]
    expires = data.get("expires_in", 3600)

    save_token(token, expires)
    print("[TOKEN] Token stored.")

    return token

# extract/sap/token_manager.py
import requests
import time
import logging
import os
from extract.sap.extract_config import (
    SAP_TOKEN_URL, CLIENT_ID, CLIENT_SECRET, TIMEOUT
)

logger = logging.getLogger(__name__)

# Global variable to store token in memory (safer than files in containers)
_TOKEN_CACHE = {
    "access_token": None,
    "expires_at": 0
}

def get_sap_token(force_refresh=False):
    """
    Retrieves a valid SAP OAuth2 token.
    Uses in-memory caching to reuse the token during this job run.
    """
    global _TOKEN_CACHE
    
    current_time = time.time()
    
    # 1. Check Cache (Reuse if valid and not expiring in next 60s)
    if not force_refresh and _TOKEN_CACHE["access_token"]:
        if current_time < (_TOKEN_CACHE["expires_at"] - 60):
            return _TOKEN_CACHE["access_token"]

    logger.info("🔑 Fetching new SAP Access Token...")
    
    # 2. Prepare Request
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    
    # 3. Call API
    try:
        response = requests.post(SAP_TOKEN_URL, data=payload, timeout=TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        access_token = data.get("access_token")
        expires_in = int(data.get("expires_in", 3600))  # Default 1 hr
        
        if not access_token:
            raise ValueError("Token response did not contain 'access_token'")
            
        # 4. Update Cache
        _TOKEN_CACHE["access_token"] = access_token
        _TOKEN_CACHE["expires_at"] = current_time + expires_in
        
        logger.info("✅ Token acquired successfully.")
        return access_token

    except Exception as e:
        logger.error(f"❌ Failed to retrieve SAP Token: {e}")
        raise e
# src/api/deps_auth.py

from fastapi import Header, HTTPException, status
import os

API_KEY = os.getenv("API_SECRET_KEY")

def verify_api_key(x_api_key: str = Header(..., description="API key for authorization")):
    """
    Validates that the provided x-api-key header matches the expected API_SECRET_KEY
    """
    if not API_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server misconfiguration: API key not set"
        )
    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    return True

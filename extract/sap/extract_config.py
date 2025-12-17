# extract/sap/extract_config.py
import os
from dotenv import load_dotenv

load_dotenv()

# --- 1. OUTPUT DIRECTORIES (Critical Fix) ---
OUTPUT_DIR = "extract/outputs"
RAW_DIR    = os.path.join(OUTPUT_DIR, "raw")
FLAT_DIR   = os.path.join(OUTPUT_DIR, "flattened")
LOG_DIR    = os.path.join(OUTPUT_DIR, "logs")

# Ensure they exist
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(FLAT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# --- 2. SAP CREDENTIALS (Mapped for Compatibility) ---
# We check for SAP_ names (Azure) first, then fall back to NADEC_ (Local)
SAP_TOKEN_URL = os.getenv("SAP_TOKEN_URL") or os.getenv("NADEC_TOKEN_URL")
SAP_PO_URL    = os.getenv("SAP_PO_URL")    or os.getenv("NADEC_PO_URL")
CLIENT_ID     = os.getenv("SAP_CLIENT_ID") or os.getenv("NADEC_CLIENT_ID")
CLIENT_SECRET = os.getenv("SAP_CLIENT_SECRET") or os.getenv("NADEC_CLIENT_SECRET")
SAP_CLIENT    = os.getenv("SAP_CLIENT", "300")

# For backward compatibility with older scripts that import specific names:
TOKEN_URL     = SAP_TOKEN_URL
NADEC_PO_URL  = SAP_PO_URL

# --- 3. PAGING & LIMITS ---
PAGE_SIZE = 100
MAX_PAGES = 2000
TIMEOUT   = 120

# --- 4. FILTERING ---
# Updated threshold based on your file
REAL_PO_THRESHOLD = 4300000000

# --- 5. HISTORICAL BATCH SETTINGS ---
# Updated for the 3-Month Pilot (Jan - Mar 2024)
HISTORICAL_START_DATE = "2024-01-01T00:00:00"
HISTORICAL_END_DATE   = "2024-04-01T00:00:00"
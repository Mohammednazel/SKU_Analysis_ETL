# extract/sap/extract_config.py
import os
from dotenv import load_dotenv
load_dotenv()

# SAP Credentials
TOKEN_URL     = os.getenv("NADEC_TOKEN_URL")
CLIENT_ID     = os.getenv("NADEC_CLIENT_ID")
CLIENT_SECRET = os.getenv("NADEC_CLIENT_SECRET")
NADEC_PO_URL  = os.getenv("NADEC_PO_URL")
SAP_CLIENT    = os.getenv("SAP_CLIENT", "300")

# Paging
PAGE_SIZE = 100
MAX_PAGES = 2000
TIMEOUT   = 30

# Filtering
REAL_PO_THRESHOLD = 4300000000

# Output directories
RAW_DIR       = "extract/outputs/raw"
FLAT_DIR      = "extract/outputs/flattened"
LOG_DIR       = "extract/outputs/logs"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(FLAT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

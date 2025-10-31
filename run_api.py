"""
run_api.py — Launcher Dashboard for FastAPI API
-----------------------------------------------
✅ Auto-loads .env
✅ Sets PYTHONPATH dynamically
✅ Starts Uvicorn server
✅ Displays API info and doc URLs
✅ Works on Windows, Linux, and macOS
"""

import os
import sys
import subprocess
from dotenv import load_dotenv
import time
import webbrowser

# -------------------------------------------------
# 1️⃣ Load environment variables
# -------------------------------------------------
load_dotenv()

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = os.getenv("API_PORT", "8000")
API_DEBUG = os.getenv("API_DEBUG", "false").lower() in ("1", "true", "yes")

# -------------------------------------------------
# 2️⃣ Set PYTHONPATH to include src
# -------------------------------------------------
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
os.environ["PYTHONPATH"] = SRC_DIR

# -------------------------------------------------
# 3️⃣ Uvicorn command builder
# -------------------------------------------------
command = [
    sys.executable, "-m", "uvicorn",
    "api.app:app",
    "--host", API_HOST,
    "--port", str(API_PORT)
]
if API_DEBUG:
    command.append("--reload")

# -------------------------------------------------
# 4️⃣ Dashboard printout
# -------------------------------------------------
display_host = "127.0.0.1" if API_HOST == "0.0.0.0" else API_HOST

print("\n" + "=" * 80)
print("🚀  Starting Procurement Instant Analytics API")
print(f"🌐  URL:        http://{display_host}:{API_PORT}")
print(f"📘  Swagger UI: http://{display_host}:{API_PORT}/docs")
print(f"📙  Redoc:      http://{display_host}:{API_PORT}/redoc")
print(f"❤️  Health:     http://{display_host}:{API_PORT}/api/v1/health")
print("-" * 80)
print(f"🧩  PYTHONPATH: {SRC_DIR}")
print(f"🐍  Python:     {sys.executable}")
print(f"⚙️  Debug Mode: {API_DEBUG}")
print("=" * 80 + "\n")

# -------------------------------------------------
# 5️⃣ Start server
# -------------------------------------------------
try:
    time.sleep(0.5)
    subprocess.run(command, check=True)
except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")
except subprocess.CalledProcessError as e:
    print(f"\n❌ Server crashed (exit code {e.returncode})")
    sys.exit(e.returncode)

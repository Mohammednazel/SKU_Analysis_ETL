import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load env vars
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();")).fetchone()
        print("✅ Connected to DB successfully!")
        print("PostgreSQL version:", result[0])
except Exception as e:
    print("❌ Connection failed:", e)

# analysis/api/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables
load_dotenv() 

user = quote_plus(os.getenv("DB_USER", ""))
pw   = quote_plus(os.getenv("DB_PASS", ""))
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
db   = os.getenv("DB_NAME", "procurement")

SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{pw}@{host}:{port}/{db}"

# Connection Pool Settings
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
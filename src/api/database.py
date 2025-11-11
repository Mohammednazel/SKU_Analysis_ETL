import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Optimized pooled engine for API read/write traffic
engine = create_engine(
    DATABASE_URL,
    pool_size=20,           # Persistent connections
    max_overflow=40,        # Allow burst connections
    pool_pre_ping=True,     # Validate before use
    pool_recycle=3600,      # Recycle every hour
    pool_timeout=30         # Wait up to 30s for a connection
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

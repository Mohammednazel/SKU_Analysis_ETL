# analysis/api/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

# 1. Fetch Credentials
user = quote_plus(os.getenv("DB_USER"))
pw   = quote_plus(os.getenv("DB_PASS", ""))
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db   = os.getenv("DB_NAME") # <--- Fixed default to postgres

# 2. Create Connection URL
SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{pw}@{host}:{port}/{db}"

# 3. Create Engine
# pool_pre_ping=True helps handle Azure connection drops gracefully
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)

# 4. Create Session Factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 5. Base Class for Models (if needed later)
Base = declarative_base()

# 6. Dependency for FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
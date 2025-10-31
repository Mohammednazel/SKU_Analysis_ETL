import os
from sqlalchemy import create_engine, text

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_FILE = os.path.join(BASE_DIR, "schema.sql")

def init_db():
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:Nazel%40123@localhost:5432/procurementdb")
    engine = create_engine(db_url)
    with open(SCHEMA_FILE, "r") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("✅ Database initialized successfully.")

if __name__ == "__main__":
    init_db()

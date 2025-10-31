import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def run():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    schema_path = os.path.join(os.path.dirname(__file__), "phase3_schema.sql")

    engine = create_engine(db_url)
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()

    with engine.begin() as conn:
        conn.execute(text(ddl))

    print("✅ Phase 3 summary MVs & tables created.")

if __name__ == "__main__":
    run()

import os
import psycopg2
from datetime import date
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
load_dotenv()


# Load ENV
DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "name": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS"),
}

PARENT_TABLES = [
    "purchase_order_headers",
    "purchase_order_items"
]

def connect():
    return psycopg2.connect(
        host=DB["host"],
        port=DB["port"],
        dbname=DB["name"],
        user=DB["user"],
        password=DB["password"],
        sslmode="require"
    )

def create_partition_sql(table, year, month):
    """
    Example partition:
    purchase_order_items_p_2024_05
    """
    partition_name = f"{table}_p_{year}_{month:02d}"

    start_date = f"{year}-{month:02d}-01"
    end_date = (date(year, month, 1) + relativedelta(months=1)).strftime("%Y-%m-%d")

    sql = f"""
    CREATE TABLE IF NOT EXISTS app_core.{partition_name}
    PARTITION OF app_core.{table}
    FOR VALUES FROM ('{start_date}') TO ('{end_date}');

    CREATE INDEX IF NOT EXISTS idx_{partition_name}_order_date
        ON app_core.{partition_name} (order_date);

    CREATE INDEX IF NOT EXISTS idx_{partition_name}_po_id
        ON app_core.{partition_name} (purchase_order_id);
    """

    return sql


def main():
    print("Connecting to DB...")
    conn = connect()
    cur = conn.cursor()

    start = date(2023, 1, 1)
    end = date.today() + relativedelta(years=5)

    print(f"Generating partitions from {start} to {end}")

    d = start
    while d <= end:
        for table in PARENT_TABLES:
            sql = create_partition_sql(table, d.year, d.month)
            try:
                cur.execute(sql)
                print(f"OK: {table}_p_{d.year}_{d.month:02d}")
            except Exception as e:
                print(f"ERR creating partition for {table}, {d.year}-{d.month:02d}: {e}")

        d += relativedelta(months=1)

    conn.commit()
    cur.close()
    conn.close()
    print("DONE. All partitions generated.")


if __name__ == "__main__":
    main()

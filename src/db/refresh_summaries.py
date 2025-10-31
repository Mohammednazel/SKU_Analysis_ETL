import os
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def refresh_all(concurrently: bool = True, snapshot: bool = False):
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))

    refresh_clause = "CONCURRENTLY " if concurrently else ""
    with engine.begin() as conn:
        # Refresh MVs
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_sku_spend;"))
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_supplier_monthly;"))
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_pgroup_spend;"))

        if snapshot:
            today = date.today()
            # Snapshot from MVs into summary tables
            conn.execute(text("""
                INSERT INTO summary_sku_spend (snapshot_date, product_id, total_spend, total_qty, order_count, avg_unit_price_weighted)
                SELECT :snap, product_id, total_spend, total_qty, order_count, avg_unit_price_weighted
                FROM mv_sku_spend
                ON CONFLICT (snapshot_date, product_id) DO UPDATE SET
                  total_spend = EXCLUDED.total_spend,
                  total_qty = EXCLUDED.total_qty,
                  order_count = EXCLUDED.order_count,
                  avg_unit_price_weighted = EXCLUDED.avg_unit_price_weighted;
            """), {"snap": today})

            conn.execute(text("""
                INSERT INTO summary_supplier_spend_monthly (snapshot_date, supplier_id, month, total_spend, po_count, unique_skus)
                SELECT :snap, supplier_id, month, total_spend, po_count, unique_skus
                FROM mv_supplier_monthly
                ON CONFLICT (snapshot_date, supplier_id, month) DO UPDATE SET
                  total_spend = EXCLUDED.total_spend,
                  po_count = EXCLUDED.po_count,
                  unique_skus = EXCLUDED.unique_skus;
            """), {"snap": today})

            conn.execute(text("""
                INSERT INTO summary_pgroup_spend (snapshot_date, purchasing_group, total_spend, po_count, avg_order_value)
                SELECT :snap, purchasing_group, total_spend, po_count, avg_order_value
                FROM mv_pgroup_spend
                ON CONFLICT (snapshot_date, purchasing_group) DO UPDATE SET
                  total_spend = EXCLUDED.total_spend,
                  po_count = EXCLUDED.po_count,
                  avg_order_value = EXCLUDED.avg_order_value;
            """), {"snap": today})

    print("✅ Refreshed materialized views.", "(snapshotted)" if snapshot else "")

if __name__ == "__main__":
    # Default: concurrent refresh, no snapshot
    refresh_all(concurrently=True, snapshot=False)

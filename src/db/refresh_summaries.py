"""
Refresh Materialized Views and Summary Tables
=============================================
Called by ETL after successful data load.

Functions:
-----------
✅ refresh_all(concurrently=True, snapshot=False)
    - Refreshes all materialized views (SKU, Supplier, PGroup, KPI)
    - Falls back to non-concurrent refresh for mv_kpi_summary if needed
    - Optionally snapshots summaries into history tables if snapshot=True
"""

import os
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def refresh_all(concurrently: bool = True, snapshot: bool = False):
    """Refresh all materialized views, with fallback for KPI view."""
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL not set in environment variables")

    engine = create_engine(db_url, pool_pre_ping=True)
    refresh_clause = "CONCURRENTLY " if concurrently else ""

    with engine.begin() as conn:
        print("🔄 Refreshing Materialized Views...")

        # 1️⃣ Core Summary Views (safe for concurrent)
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_sku_spend;"))
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_supplier_monthly;"))
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_pgroup_spend;"))
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_supplier_price_analysis;"))
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_spend_trend_monthly;"))
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_sku_analysis;"))

        # 2️⃣ KPI Summary View (may fail concurrently — fallback)
                # 2️⃣ KPI Summary View (may fail concurrently — fallback)
        try:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_clause}mv_kpi_summary;"))
            print("✅ Refreshed mv_kpi_summary (concurrent)")
        except Exception as e:
            print(f"⚠️ Concurrent refresh failed for mv_kpi_summary: {e}")
            print("➡️ Retrying non-concurrent refresh in new transaction...")

            # Rollback current failed transaction and open a clean one
            conn.rollback()
            with engine.begin() as fallback_conn:
                fallback_conn.execute(text("REFRESH MATERIALIZED VIEW mv_kpi_summary;"))
                print("✅ Refreshed mv_kpi_summary (non-concurrent fallback)")

        # 3️⃣ Optional: Snapshot historical trends
        if snapshot:
            today = date.today()
            print(f"📸 Taking daily snapshot for {today}...")

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

    print("🏁 All materialized views refreshed successfully.", "(snapshotted)" if snapshot else "")


if __name__ == "__main__":
    refresh_all(concurrently=True, snapshot=False)

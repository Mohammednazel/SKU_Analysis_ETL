"""
refresh_summaries.py — Production-Optimized Version
===================================================
Purpose:
--------
Triggered after ETL ingestion completes successfully.

✅ Concurrent refresh with per-view fallback
✅ Avoids redundant calls
✅ Handles missing indexes gracefully
✅ Logs per-view duration
✅ Maintains snapshot feature
"""

import os
import time
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def refresh_view(conn, view_name: str, concurrent=True):
    """Safely refresh one materialized view, with optional fallback."""
    start = time.time()
    clause = "CONCURRENTLY " if concurrent else ""
    try:
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {clause}{view_name};"))
        print(f"✅ Refreshed {view_name} ({'concurrent' if concurrent else 'non-concurrent'}) "
              f"in {time.time() - start:.2f}s")
    except Exception as e:
        print(f"⚠️ Failed concurrent refresh for {view_name}: {e}")
        print(f"➡️ Retrying {view_name} non-concurrently...")
        conn.rollback()
        conn.execute(text(f"REFRESH MATERIALIZED VIEW {view_name};"))
        print(f"✅ Refreshed {view_name} (fallback) in {time.time() - start:.2f}s")


def refresh_all(concurrently: bool = True, snapshot: bool = False):
    """Refresh all materialized views, with fallback for KPI view."""
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL not set in environment variables")

    engine = create_engine(
    db_url,
    pool_size=5,          # Smaller pool is fine for ETL jobs
    max_overflow=10,      # Occasional temporary bursts
    pool_pre_ping=True,   # Keep connections healthy
    pool_recycle=1800,    # Recycle every 30 min
    pool_timeout=60       # Wait up to 1 minute for connection
)
    with engine.begin() as conn:
        print("🔄 Refreshing Materialized Views...\n")

        core_views = [
            "mv_sku_spend",
            "mv_supplier_monthly",
            "mv_pgroup_spend",
            "mv_supplier_price_analysis",
            "mv_spend_trend_monthly",
            "mv_sku_analysis",
            "mv_sku_fragmentation_score",
            "mv_contract_candidates",
            "mv_supplier_consolidation",
            "mv_volume_discount_opportunities",
            "mv_kpi_summary" ,
            
        ]

        for view in core_views:
            refresh_view(conn, view, concurrent=concurrently)

       
        # Snapshot optional
        if snapshot:
            today = date.today()
            print(f"\n📸 Taking daily snapshot for {today}...")

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

            print("✅ Snapshot completed successfully.")

    print("\n🏁 All materialized views refreshed successfully.",
          "(Snapshots enabled)" if snapshot else "(No snapshots)")
    print("===========================================================")


if __name__ == "__main__":
    refresh_all(concurrently=True, snapshot=False)

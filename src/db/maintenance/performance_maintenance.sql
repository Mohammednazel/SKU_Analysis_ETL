-- # DB vacuum/analyze/reindex maintenance script

-- =====================================================
-- DATABASE PERFORMANCE MAINTENANCE SCRIPT
-- For: Procurement SKU Frequency & Spend Analysis
-- Author: [Your Name]
-- Description:
--   Keeps PostgreSQL optimized for large-scale analytics
--   Run Weekly or Monthly in production
-- =====================================================

\echo '🔧 Starting database maintenance...'

-- 1️⃣ Recompute planner statistics
\echo '🧮 Running ANALYZE on purchase_orders and views...'
ANALYZE purchase_orders;
ANALYZE mv_sku_spend;
ANALYZE mv_supplier_monthly;
ANALYZE mv_pgroup_spend;

-- 2️⃣ Vacuum to reclaim space
\echo '🧹 Running VACUUM (FULL optional for deep clean)...'
VACUUM (VERBOSE, ANALYZE) purchase_orders;
VACUUM (VERBOSE, ANALYZE) mv_sku_spend;
VACUUM (VERBOSE, ANALYZE) mv_supplier_monthly;
VACUUM (VERBOSE, ANALYZE) mv_pgroup_spend;

-- 3️⃣ Check index bloat (optional)
\echo '📏 Checking index bloat (optional diagnostic)...'
SELECT
    schemaname, relname AS table_name, indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size
FROM pg_stat_user_indexes i
JOIN pg_index x ON x.indexrelid = i.indexrelid
ORDER BY pg_relation_size(i.indexrelid) DESC
LIMIT 10;

-- 4️⃣ Reindex large / bloated indexes if needed
\echo '🔄 Reindexing (run monthly if bloat > 20%)...'
REINDEX TABLE purchase_orders;
REINDEX TABLE mv_sku_spend;
REINDEX TABLE mv_supplier_monthly;
REINDEX TABLE mv_pgroup_spend;

-- 5️⃣ Refresh materialized views to ensure freshness
\echo '♻️ Refreshing all materialized views...'
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sku_spend;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_supplier_monthly;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_pgroup_spend;

-- 6️⃣ Summarize table sizes (for monitoring)
\echo '📊 Current table size summary:'
SELECT
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;


\echo '📈 ETL performance summary:'
SELECT
    run_id,
    mode,
    start_time,
    end_time,
    status,
    rows_processed,
    ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::numeric, 2) AS duration_seconds
FROM etl_run_log
ORDER BY run_id DESC
LIMIT 10;

\echo '✅ Database maintenance completed successfully.'

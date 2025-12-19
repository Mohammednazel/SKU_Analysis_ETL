
DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_global_kpis CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_global_kpis AS
SELECT
    COUNT(DISTINCT purchase_order_id)  AS total_orders,
    COUNT(DISTINCT unified_sku_id)      AS total_skus,
    COUNT(DISTINCT supplier_name)       AS total_suppliers,
    SUM(total)                          AS total_spend,
    MIN(order_date)                     AS first_order_date,
    MAX(order_date)                     AS last_order_date
FROM app_analytics.v_items_enriched;
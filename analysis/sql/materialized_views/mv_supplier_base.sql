DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_supplier_base CASCADE;


CREATE MATERIALIZED VIEW app_analytics.mv_supplier_base AS
SELECT
    supplier_name,
    COUNT(DISTINCT purchase_order_id) AS order_count,
    COUNT(DISTINCT unified_sku_id)    AS sku_count,
    SUM(total)                        AS total_spend
FROM app_analytics.v_items_enriched
GROUP BY supplier_name;
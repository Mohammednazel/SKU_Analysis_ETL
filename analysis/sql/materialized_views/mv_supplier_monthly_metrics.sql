DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_supplier_monthly_metrics CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_supplier_monthly_metrics AS
SELECT
    supplier_name,
    order_month,
    order_year,

    COUNT(DISTINCT purchase_order_id) AS order_count,
    COUNT(DISTINCT unified_sku_id)    AS sku_count,
    SUM(total)                        AS total_spend
FROM app_analytics.v_items_enriched
GROUP BY supplier_name, order_month, order_year;

DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_sku_monthly_metrics CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_sku_monthly_metrics AS
SELECT
    unified_sku_id,
    sku_name,
    order_month,
    order_year,

    COUNT(DISTINCT purchase_order_id) AS order_count,
    SUM(quantity)                     AS total_quantity,
    SUM(total)                        AS total_spend
FROM app_analytics.v_items_enriched
GROUP BY unified_sku_id, sku_name, order_month, order_year;
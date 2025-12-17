DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_sku_weekly_metrics CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_sku_weekly_metrics AS
SELECT
    unified_sku_id,
    sku_name,

    -- Week dimension
    DATE_TRUNC('week', order_date)::date AS order_week,
    EXTRACT(YEAR FROM order_date)::int AS order_year,

    -- Core procurement metrics
    SUM(total)        AS weekly_spend,
    SUM(quantity)     AS weekly_quantity,
    COUNT(DISTINCT purchase_order_id) AS weekly_order_count,

    -- Risk & behavior indicators
    COUNT(DISTINCT supplier_name) AS supplier_count,

    -- Price behavior
    AVG(unit_price) AS avg_unit_price

FROM app_analytics.v_items_enriched
GROUP BY
    unified_sku_id,
    sku_name,
    DATE_TRUNC('week', order_date),
    EXTRACT(YEAR FROM order_date);

DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_sku_contract_base CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_sku_contract_base AS
SELECT
    unified_sku_id,
    MIN(TRIM(sku_name)) AS sku_name,
    unit_of_measure,

    COUNT(DISTINCT purchase_order_id) AS order_count,
    COUNT(DISTINCT order_month)       AS active_months,
    COUNT(DISTINCT supplier_name)     AS supplier_count,

    -- ✅ SAR-based financials
    SUM(total_sar)                    AS total_spend_sar,
    SUM(quantity)                     AS total_quantity,
    AVG(unit_price_sar)               AS avg_unit_price_sar,
    STDDEV(unit_price_sar)            AS price_stddev_sar

FROM app_analytics.v_items_enriched
GROUP BY
    unified_sku_id,
    unit_of_measure;

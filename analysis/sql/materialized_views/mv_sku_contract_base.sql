DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_sku_contract_base CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_sku_contract_base AS
SELECT
    unified_sku_id,
    sku_name,
    unit_of_measure,

    -- Frequency & stability signals
    COUNT(DISTINCT purchase_order_id) AS order_count,
    COUNT(DISTINCT order_month)       AS active_months,
    COUNT(DISTINCT supplier_name)     AS supplier_count,

    -- Financials (UOM-specific)
    SUM(total)        AS total_spend,
    SUM(quantity)     AS total_quantity,

    -- Price behavior (SAFE: computed within same UOM)
    AVG(unit_price)   AS avg_unit_price,
    STDDEV(unit_price) AS price_stddev

FROM app_analytics.v_items_enriched
GROUP BY
    unified_sku_id,
    sku_name,
    unit_of_measure;
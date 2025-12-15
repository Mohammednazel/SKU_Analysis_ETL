CREATE MATERIALIZED VIEW app_analytics.mv_sku_price_variance AS
SELECT
    unified_sku_id,
    sku_name,
    supplier_name,

    COUNT(*)                      AS line_count,
    AVG(unit_price)               AS avg_unit_price,
    STDDEV(unit_price)            AS price_stddev,
    MIN(unit_price)               AS min_price,
    MAX(unit_price)               AS max_price
FROM app_analytics.v_items_enriched
WHERE unit_price > 0
GROUP BY unified_sku_id, sku_name, supplier_name;

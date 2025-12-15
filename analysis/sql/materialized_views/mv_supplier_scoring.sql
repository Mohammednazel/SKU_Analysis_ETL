CREATE MATERIALIZED VIEW app_analytics.mv_supplier_scoring AS
SELECT
    *,
    NTILE(10) OVER (ORDER BY total_spend) * 10 AS spend_score,
    NTILE(10) OVER (ORDER BY sku_count) * 10   AS sku_dependency_score
FROM app_analytics.mv_supplier_base;

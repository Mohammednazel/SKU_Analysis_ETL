-- Base analytics
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_sku_contract_base;
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_supplier_base;

-- Monthly metrics
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_sku_monthly_metrics;
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_supplier_monthly_metrics;

-- Risk & pricing
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_sku_price_variance;

-- Contract intelligence
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_contract_scoring;
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_contract_candidates;

-- Supplier intelligence
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_supplier_scoring;
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_supplier_tiering;

-- KPIs
REFRESH MATERIALIZED VIEW CONCURRENTLY app_analytics.mv_global_kpis;

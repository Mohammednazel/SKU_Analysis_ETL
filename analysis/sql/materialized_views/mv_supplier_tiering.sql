DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_supplier_tiering CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_supplier_tiering AS
SELECT
    supplier_name,
    total_spend,
    sku_count,
    order_count,

    CASE
        WHEN spend_score >= 80 THEN 'TIER_A_STRATEGIC'
        WHEN spend_score >= 40 THEN 'TIER_B_TACTICAL'
        ELSE 'TIER_C_TRANSACTIONAL'
    END AS supplier_tier,

    -- 🚨 FIX: Changed alias from 'dependency_risk' to 'dependency_risk_level'
    CASE
        WHEN sku_dependency_score >= 80 THEN 'HIGH_DEPENDENCY_RISK'
        WHEN sku_dependency_score >= 40 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS dependency_risk_level

FROM app_analytics.mv_supplier_scoring;
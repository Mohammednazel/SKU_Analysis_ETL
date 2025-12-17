DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_contract_candidates CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_contract_candidates AS
WITH scored AS (
    SELECT
        unified_sku_id,
        sku_name,
        total_spend,
        active_months,
        supplier_count,
        
        -- 🚨 FIX 1: Handle Nulls for single-order items
        COALESCE(avg_unit_price, 0) AS avg_unit_price,
        COALESCE(price_stddev, 0)   AS price_stddev,
        
        frequency_score,
        materiality_score,
        volatility_score,
        fragmentation_score,

        ROUND(
            materiality_score * 0.40 +
            frequency_score   * 0.30 +
            volatility_score  * 0.20 +
            fragmentation_score * 0.10
        ) AS contract_priority_score
    FROM app_analytics.mv_contract_scoring
)
SELECT 
    *,
    CASE 
        WHEN contract_priority_score >= 80 THEN 'STRATEGIC CONTRACT'
        WHEN contract_priority_score >= 60 THEN 'NEGOTIATE'
        WHEN contract_priority_score >= 40 THEN 'MONITOR'
        ELSE 'SPOT BUY'
    END AS contract_recommendation
FROM scored;

-- Re-create the index
CREATE INDEX idx_contract_rec ON app_analytics.mv_contract_candidates(contract_priority_score DESC);
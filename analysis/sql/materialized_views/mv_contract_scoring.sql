DROP MATERIALIZED VIEW IF EXISTS app_analytics.mv_contract_scoring CASCADE;

CREATE MATERIALIZED VIEW app_analytics.mv_contract_scoring AS
SELECT
    *,

    /* Frequency score */
    LEAST(100, active_months * 10) AS frequency_score,

    /* Spend percentile */
    NTILE(10) OVER (ORDER BY total_spend) * 10 AS materiality_score,

    /* Price volatility */
    COALESCE(
        NTILE(10) OVER (
            ORDER BY (price_stddev / NULLIF(avg_unit_price,0))
        ) * 10,
        0
    ) AS volatility_score,

    /* Supplier fragmentation */
    LEAST(100, supplier_count * 20) AS fragmentation_score

FROM app_analytics.mv_sku_contract_base;

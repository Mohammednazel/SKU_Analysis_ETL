DROP VIEW IF EXISTS app_analytics.v_items_enriched CASCADE;

CREATE VIEW app_analytics.v_items_enriched AS
SELECT
    i.purchase_order_id,
    i.purchase_order_no,

    CASE
        WHEN i.item_id IS NOT NULL AND i.item_id <> '' THEN i.item_id
        ELSE 'DESC:' || UPPER(TRIM(i.description))
    END AS unified_sku_id,

    COALESCE(TRIM(i.description), 'NO DESCRIPTION') AS sku_name,
    h.supplier_name,

    COALESCE(i.quantity, 0) AS quantity,
    COALESCE(i.unit_price, 0) AS unit_price,
    COALESCE(i.total, 0) AS total,

    -- ✅ SAR-normalized values
    COALESCE(i.total, 0) * fx.rate_to_sar      AS total_sar,
    COALESCE(i.unit_price, 0) * fx.rate_to_sar AS unit_price_sar,

    COALESCE(UPPER(TRIM(i.unit_of_measure)), 'UNT') AS unit_of_measure,
    h.currency,

    i.order_date,
    DATE_TRUNC('month', i.order_date)::date AS order_month,
    EXTRACT(YEAR FROM i.order_date)::int AS order_year,
    h.status AS order_status

FROM app_core.purchase_order_items i
JOIN app_analytics.v_headers_enriched h
  ON i.purchase_order_id = h.purchase_order_id
LEFT JOIN app_core.fx_rates fx
  ON fx.currency = h.currency
 AND i.order_date::date BETWEEN fx.valid_from AND fx.valid_to;

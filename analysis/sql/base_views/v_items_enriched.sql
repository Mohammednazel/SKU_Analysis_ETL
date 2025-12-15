CREATE OR REPLACE VIEW app_analytics.v_items_enriched AS
SELECT
    i.purchase_order_id,
    i.purchase_order_no,

    -- Unified SKU ID (Already done)
    CASE
        WHEN i.item_id IS NOT NULL AND i.item_id <> '' THEN i.item_id
        ELSE 'DESC:' || UPPER(TRIM(i.description))
    END AS unified_sku_id,

    COALESCE(i.description, 'NO DESCRIPTION') AS sku_name,

    -- Supplier Name from Clean Header View
    h.supplier_name, 

    -- 6. Null-Safe Numbers (NEW)
    COALESCE(i.quantity, 0) AS quantity,
    COALESCE(i.unit_price, 0) AS unit_price,
    COALESCE(i.total, 0) AS total,

    -- 7. Standardized UOM (NEW)
    COALESCE(UPPER(TRIM(i.unit_of_measure)), 'UNT') AS unit_of_measure,

    -- Clean Currency from Header
    h.currency,

    i.order_date,
    DATE_TRUNC('month', i.order_date)::date AS order_month,
    EXTRACT(YEAR FROM i.order_date)::int AS order_year

FROM app_core.purchase_order_items i
JOIN app_analytics.v_headers_enriched h
  ON i.purchase_order_id = h.purchase_order_id;
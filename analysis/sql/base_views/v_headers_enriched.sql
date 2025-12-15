CREATE OR REPLACE VIEW app_analytics.v_headers_enriched AS
SELECT
    purchase_order_id,

    -- 1. Standardized Supplier Name (Already done, keeping it)
    COALESCE(NULLIF(UPPER(TRIM(supplier_company_name)), ''), 'UNKNOWN SUPPLIER') AS supplier_name,

    -- 2. Standardized Buyer Name (NEW)
    COALESCE(NULLIF(UPPER(TRIM(buyer_company_name)), ''), 'UNKNOWN BUYER') AS buyer_company_name,

    -- 3. Standardized Status (NEW)
    COALESCE(UPPER(TRIM(status)), 'UNKNOWN') AS status,

    -- 4. Clean Currency Code (NEW - Critical for logic)
    COALESCE(UPPER(TRIM(currency)), 'SAR') AS currency,

    -- 5. Null-Safe Amounts (NEW)
    COALESCE(grand_amount, 0) AS grand_amount,

    order_date,
    DATE_TRUNC('month', order_date)::date AS order_month,
    EXTRACT(YEAR FROM order_date)::int AS order_year

FROM app_core.purchase_order_headers;
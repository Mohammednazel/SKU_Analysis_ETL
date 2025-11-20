-- =====================================================================
--  Database Initialization Script for Procurement SKU Frequency Analysis
-- =====================================================================
--  This script bootstraps the database schema automatically when the
--  PostgreSQL container starts for the first time.
--  It includes:
--     1️⃣ Core transactional tables
--     2️⃣ ETL metadata / audit tables
--     3️⃣ Summary materialized views
--     4️⃣ Optional maintenance and indexes
-- =====================================================================

\echo '🚀 Initializing ProcurementDB schema...'

-- =============================
-- 1️⃣ Core Table: Purchase Orders
-- =============================
CREATE TABLE IF NOT EXISTS purchase_orders (
    purchase_order_id TEXT NOT NULL,
    line_item_number TEXT NOT NULL,
    created_date TIMESTAMP WITH TIME ZONE,
    status TEXT,
    supplier_id TEXT,
    purchasing_group TEXT,
    plant TEXT,
    product_id TEXT,
    description TEXT,
    quantity NUMERIC,
    unit TEXT,
    unit_price NUMERIC,
    net_value NUMERIC,
    material_group TEXT,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    source_hash TEXT,
    PRIMARY KEY (purchase_order_id, line_item_number)
);

-- Indexes for filters & aggregation performance
CREATE INDEX IF NOT EXISTS idx_po_created_date ON purchase_orders(created_date);
CREATE INDEX IF NOT EXISTS idx_po_supplier_id ON purchase_orders(supplier_id);
CREATE INDEX IF NOT EXISTS idx_po_product_id ON purchase_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_po_material_group ON purchase_orders(material_group);

\echo '✅ purchase_orders table created.'


-- =====================================
-- 2️⃣ ETL Metadata Tables (Checkpoints & Locks)
-- =====================================
CREATE TABLE IF NOT EXISTS etl_checkpoint (
    id SERIAL PRIMARY KEY,
    job_name TEXT UNIQUE NOT NULL,
    last_offset INT NOT NULL DEFAULT 0,
    last_run TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id SERIAL PRIMARY KEY,
    mode TEXT,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    rows_processed INT,
    rows_inserted INT,
    rows_updated INT,
    status TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS etl_lock (
    job_name TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ DEFAULT now(),
    status TEXT DEFAULT 'running'
);


\echo '✅ ETL metadata tables created.'




-- =============================
-- 3️⃣ Phase 3: Summary Layer
-- =============================

-- SKU-level spend summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sku_spend AS
SELECT
  product_id,
  COALESCE(SUM(net_value), 0)::numeric        AS total_spend,
  COALESCE(SUM(quantity), 0)::numeric         AS total_qty,
  COUNT(DISTINCT purchase_order_id)::int      AS order_count,
  CASE WHEN COALESCE(SUM(quantity),0) > 0
       THEN (SUM(net_value)::numeric / NULLIF(SUM(quantity),0))
       ELSE 0 END                             AS avg_unit_price_weighted,
  MAX(created_date)                           AS last_order_date
FROM purchase_orders
GROUP BY product_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sku_spend_product
  ON mv_sku_spend(product_id);

-- Supplier x Month Spend Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_supplier_monthly AS
SELECT
  supplier_id,
  DATE_TRUNC('month', created_date)::date     AS month,
  COALESCE(SUM(net_value),0)::numeric         AS total_spend,
  COUNT(DISTINCT purchase_order_id)::int      AS po_count,
  COUNT(DISTINCT product_id)::int             AS unique_skus
FROM purchase_orders
GROUP BY supplier_id, DATE_TRUNC('month', created_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_supplier_monthly
  ON mv_supplier_monthly(supplier_id, month);

-- Purchasing Group Spend
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pgroup_spend AS
SELECT
  purchasing_group,
  COALESCE(SUM(net_value),0)::numeric         AS total_spend,
  COUNT(DISTINCT purchase_order_id)::int      AS po_count,
  CASE WHEN COUNT(DISTINCT purchase_order_id) > 0
       THEN SUM(net_value)::numeric / COUNT(DISTINCT purchase_order_id)
       ELSE 0 END                             AS avg_order_value
FROM purchase_orders
GROUP BY purchasing_group;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pgroup_spend
  ON mv_pgroup_spend(purchasing_group);

\echo '✅ Materialized views created.'


-- Materialized View: Global KPI Summary

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kpi_summary AS
SELECT 
    1 As id ,
    COUNT(DISTINCT purchase_order_id)                        AS total_pos,
    COUNT(DISTINCT product_id)                               AS total_skus,
    COUNT(DISTINCT supplier_id)                              AS total_suppliers,
    COALESCE(SUM(net_value), 0)::numeric                     AS total_spend,
    COALESCE(SUM(quantity), 0)::numeric                      AS total_quantity,
    ROUND(COALESCE(SUM(net_value) / NULLIF(SUM(quantity), 0), 0), 2) AS avg_unit_price_weighted,
    ROUND(COALESCE(SUM(net_value) / NULLIF(COUNT(DISTINCT purchase_order_id), 0), 0), 2) AS avg_order_value,
    ROUND(COALESCE(SUM(net_value) / NULLIF(COUNT(DISTINCT product_id), 0), 0), 2) AS spend_per_sku,
    ROUND(COALESCE(MAX(net_value) / NULLIF(MIN(net_value), 0), 1), 2)             AS spend_variability_ratio,
    MAX(created_date)                                                             AS last_po_date,
    now()                                                                         AS last_refresh_time
FROM purchase_orders;

-- Required for CONCURRENT REFRESH
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_kpi_summary_unique
  ON mv_kpi_summary (id);


-- ==========================================================
-- Monthly Spend Trend (All Suppliers)
-- ==========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_spend_trend_monthly AS
SELECT
    DATE_TRUNC('month', created_date)::DATE AS month,
    SUM(net_value)::NUMERIC AS total_spend,
    SUM(quantity)::NUMERIC AS total_qty,
    COUNT(DISTINCT purchase_order_id) AS total_pos
FROM purchase_orders
GROUP BY DATE_TRUNC('month', created_date)
ORDER BY month;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_spend_trend_monthly
    ON mv_spend_trend_monthly (month);

\echo '✅ mv_spend_trend_monthly created.'


-- ==========================================================
-- Supplier Price Analysis (Avg Unit Price per SKU per Supplier)
-- ==========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_supplier_price_analysis AS
SELECT
    supplier_id,
    product_id,
    COUNT(DISTINCT purchase_order_id) AS po_count,
    SUM(quantity)::NUMERIC AS total_qty,
    SUM(net_value)::NUMERIC AS total_spend,
    CASE 
        WHEN SUM(quantity) > 0 THEN ROUND(SUM(net_value) / SUM(quantity), 2)
        ELSE 0 
    END AS avg_unit_price,
    MAX(created_date) AS last_purchase_date
FROM purchase_orders
WHERE supplier_id IS NOT NULL
GROUP BY supplier_id, product_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_supplier_price_unique
    ON mv_supplier_price_analysis (supplier_id, product_id);

\echo '✅ mv_supplier_price_analysis created.'


-- ==========================================================
-- Detailed SKU Analysis (SKU x Supplier x Purchasing Group)
-- Aggregates: totals, weighted price, last purchase
-- ==========================================================

-- For fast fuzzy search (optional but recommended)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sku_analysis AS
WITH items AS (
    SELECT
        product_id,
        supplier_id,
        purchasing_group,
        -- choose a stable description (latest non-null)
        (SELECT i2.description
         FROM purchase_orders i2
         WHERE i2.product_id = po.product_id
         AND   i2.description IS NOT NULL
         ORDER BY i2.created_date DESC
         LIMIT 1) AS description,
        COUNT(DISTINCT purchase_order_id)              AS order_count,
        SUM(quantity)::NUMERIC                         AS total_qty,
        SUM(net_value)::NUMERIC                        AS total_spend,
        CASE
            WHEN SUM(quantity) > 0
                THEN ROUND(SUM(net_value) / NULLIF(SUM(quantity),0), 2)
            ELSE 0
        END                                            AS avg_unit_price,
        MAX(created_date)                              AS last_order_date
    FROM purchase_orders po
    WHERE product_id IS NOT NULL
    GROUP BY product_id, supplier_id, purchasing_group
)
SELECT
    product_id,
    COALESCE(description, '')                         AS description,
    purchasing_group,
    supplier_id,
    order_count,
    total_qty,
    total_spend,
    avg_unit_price,
    last_order_date
FROM items;

-- Unique row identity for concurrent refresh support
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sku_analysis_unique
    ON mv_sku_analysis (product_id, supplier_id, purchasing_group);

-- Filter indexes
CREATE INDEX IF NOT EXISTS idx_mv_sku_analysis_pgroup
    ON mv_sku_analysis (purchasing_group);

CREATE INDEX IF NOT EXISTS idx_mv_sku_analysis_supplier
    ON mv_sku_analysis (supplier_id);

-- Fast search on product_id + description
CREATE INDEX IF NOT EXISTS idx_mv_sku_analysis_desc_trgm
    ON mv_sku_analysis USING gin (description gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_mv_sku_analysis_pid_trgm
    ON mv_sku_analysis USING gin (product_id gin_trgm_ops);

\echo '✅ mv_sku_analysis created.'


-- ==========================================================
-- mv_contract_candidates (corrected)
-- ==========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_contract_candidates AS
WITH monthly_orders AS (
    SELECT
        product_id,
        supplier_id,
        DATE_TRUNC('month', created_date)::date AS month,
        COUNT(DISTINCT purchase_order_id) AS orders_in_month,
        SUM(net_value)::numeric AS monthly_spend
    FROM purchase_orders
    WHERE product_id IS NOT NULL AND supplier_id IS NOT NULL
    GROUP BY product_id, supplier_id, DATE_TRUNC('month', created_date)
),
stats AS (
    SELECT
        product_id,
        supplier_id,
        COUNT(DISTINCT month) AS active_months,
        SUM(orders_in_month) AS total_orders,
        SUM(monthly_spend) AS total_spend,
        -- months_observed: inclusive months between min and max month
        (
            (EXTRACT(year FROM MAX(month)) - EXTRACT(year FROM MIN(month)))::int * 12
            + (EXTRACT(month FROM MAX(month)) - EXTRACT(month FROM MIN(month)))::int
            + 1
        )::int AS months_observed
    FROM monthly_orders
    GROUP BY product_id, supplier_id
)
SELECT
    s.product_id,
    s.supplier_id,
    -- avg_orders_per_month as numeric, rounded to 2 decimals
    ROUND(
        (s.total_orders::numeric) /
        NULLIF(s.months_observed::numeric, 0)
    , 2) AS avg_orders_per_month,
    -- purchase_consistency_pct = 100 * active_months / months_observed
    ROUND(
        100.0 * (s.active_months::numeric) /
        NULLIF(s.months_observed::numeric, 0)
    , 2) AS purchase_consistency_pct,
    CASE
        WHEN ROUND((s.total_orders::numeric) / NULLIF(s.months_observed::numeric,0), 2) >= 10 THEN 'VERY_HIGH'
        WHEN ROUND((s.total_orders::numeric) / NULLIF(s.months_observed::numeric,0), 2) >= 5 THEN 'HIGH'
        WHEN ROUND((s.total_orders::numeric) / NULLIF(s.months_observed::numeric,0), 2) >= 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS purchase_frequency,
    CASE
        WHEN ROUND(100.0 * (s.active_months::numeric) / NULLIF(s.months_observed::numeric,0), 2) >= 75
             AND s.total_spend > 100000 THEN 'ANNUAL_CONTRACT_RECOMMENDED'
        WHEN ROUND(100.0 * (s.active_months::numeric) / NULLIF(s.months_observed::numeric,0), 2) >= 50
             AND s.total_spend > 20000 THEN 'QUARTERLY_CONTRACT_RECOMMENDED'
        WHEN ROUND(100.0 * (s.active_months::numeric) / NULLIF(s.months_observed::numeric,0), 2) >= 75 THEN 'BLANKET_PO_RECOMMENDED'
        ELSE 'SPOT_BUY_OK'
    END AS contract_recommendation,
    -- annual_spend_projected: scale observed spend to annual if needed
    ROUND(
        (s.total_spend::numeric) * 12.0 / NULLIF(s.months_observed::numeric, 0)
    , 2) AS annual_spend_projected
FROM stats s;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_contract_candidates
    ON mv_contract_candidates (product_id, supplier_id);



-- ==========================================================
-- mv_supplier_consolidation (corrected, robust)
-- ==========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_supplier_consolidation AS
WITH supplier_stats AS (
    SELECT
        supplier_id,
        COUNT(DISTINCT product_id) AS unique_skus,
        SUM(net_value)::numeric AS total_spend
    FROM purchase_orders
    WHERE supplier_id IS NOT NULL
    GROUP BY supplier_id
),
ranked AS (
    SELECT
        supplier_id,
        unique_skus,
        total_spend,
        RANK() OVER (ORDER BY total_spend DESC) AS spend_rank,
        SUM(total_spend) OVER () AS grand_total
    FROM supplier_stats
)
SELECT
    r.supplier_id,
    r.unique_skus,
    r.total_spend,
    r.spend_rank,
    ROUND(100.0 * r.total_spend / NULLIF(r.grand_total, 0), 4) AS spend_pct,
    CASE
        WHEN r.spend_rank <= 5 THEN 'TOP_5_STRATEGIC'
        WHEN r.spend_rank <= 50 THEN 'MID_TIER'
        ELSE 'LONG_TAIL_CONSOLIDATE'
    END AS supplier_tier,
    CASE
        WHEN r.spend_rank > 50
             AND (100.0 * r.total_spend / NULLIF(r.grand_total, 0)) < 0.5
            THEN 'CONSOLIDATE_TO_TOP_SUPPLIERS'
        ELSE 'KEEP'
    END AS consolidation_action
FROM ranked r;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_supplier_consolidation
    ON mv_supplier_consolidation (supplier_id);


-- ==========================================================
-- VOLUME DISCOUNT / BEST SUPPLIER OPPORTUNITIES (FIXED)
-- ==========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_volume_discount_opportunities AS
WITH supplier_prices AS (
    SELECT
        product_id,
        supplier_id,
        ROUND(AVG(unit_price)::numeric, 2) AS avg_unit_price,
        SUM(net_value)::numeric AS total_spend,
        SUM(quantity)::numeric AS total_qty
    FROM purchase_orders
    WHERE unit_price IS NOT NULL AND quantity > 0
    GROUP BY product_id, supplier_id
),
ranked AS (
    SELECT
        product_id,
        supplier_id,
        avg_unit_price,
        total_spend,
        total_qty,
        RANK() OVER (PARTITION BY product_id ORDER BY avg_unit_price ASC) AS price_rank
    FROM supplier_prices
),
best_prices AS (
    SELECT
        product_id,
        supplier_id AS best_supplier_id,
        avg_unit_price AS best_unit_price
    FROM ranked
    WHERE price_rank = 1
)
SELECT
    md5(r.product_id || '-' || r.supplier_id || '-' || b.best_supplier_id)::uuid AS id,
    r.product_id,
    r.supplier_id AS current_supplier_id,
    r.avg_unit_price AS current_avg_price,
    b.best_supplier_id,
    b.best_unit_price,
    ROUND((r.avg_unit_price - b.best_unit_price) * r.total_qty, 2) AS potential_savings,
    ROUND(100.0 * (r.avg_unit_price - b.best_unit_price) / NULLIF(r.avg_unit_price, 0), 2) AS savings_pct,
    CASE
        WHEN (r.avg_unit_price - b.best_unit_price) * r.total_qty >= 10000 THEN 'HIGH_SAVINGS_OPPORTUNITY'
        WHEN (r.avg_unit_price - b.best_unit_price) * r.total_qty >= 1000 THEN 'MEDIUM_SAVINGS_OPPORTUNITY'
        ELSE 'LOW_SAVINGS_OPPORTUNITY'
    END AS opportunity_level
FROM ranked r
JOIN best_prices b USING (product_id)
WHERE r.price_rank > 1 AND r.avg_unit_price > b.best_unit_price;

-- ✅ Unique index on surrogate key
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_volume_discount_opportunities
    ON mv_volume_discount_opportunities (id);

\echo '✅ mv_volume_discount_opportunities created successfully (deduplicated).'


-- ==========================================================
-- SKU FRAGMENTATION SCORE
-- ==========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sku_fragmentation_score AS
SELECT
    product_id,
    COUNT(DISTINCT supplier_id) AS supplier_count,
    SUM(net_value)::numeric AS total_spend,
    ROUND(
        100.0 * COUNT(DISTINCT supplier_id)::numeric / NULLIF(SUM(net_value)::numeric, 0),
        4
    ) AS fragmentation_score,
    CASE
        WHEN COUNT(DISTINCT supplier_id) > 5 THEN 'HIGHLY_FRAGMENTED'
        WHEN COUNT(DISTINCT supplier_id) BETWEEN 3 AND 5 THEN 'MODERATELY_FRAGMENTED'
        ELSE 'CONSOLIDATED'
    END AS fragmentation_level
FROM purchase_orders
WHERE supplier_id IS NOT NULL
GROUP BY product_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sku_fragmentation_score
    ON mv_sku_fragmentation_score (product_id);
\echo '✅ mv_sku_fragmentation_score created.'




-- =============================
-- 4️⃣ Snapshot Tables for MV Audits
-- =============================
CREATE TABLE IF NOT EXISTS summary_sku_spend (
  snapshot_date date NOT NULL,
  product_id text NOT NULL,
  total_spend numeric,
  total_qty numeric,
  order_count int,
  avg_unit_price_weighted numeric,
  PRIMARY KEY (snapshot_date, product_id)
);

CREATE TABLE IF NOT EXISTS summary_supplier_spend_monthly (
  snapshot_date date NOT NULL,
  supplier_id text NOT NULL,
  month date NOT NULL,
  total_spend numeric,
  po_count int,
  unique_skus int,
  PRIMARY KEY (snapshot_date, supplier_id, month)
);

CREATE TABLE IF NOT EXISTS summary_pgroup_spend (
  snapshot_date date NOT NULL,
  purchasing_group text NOT NULL,
  total_spend numeric,
  po_count int,
  avg_order_value numeric,
  PRIMARY KEY (snapshot_date, purchasing_group)
);

\echo '✅ Snapshot summary tables created.'


-- =============================
-- 5️⃣ Optional: Initial Maintenance
-- =============================
ANALYZE purchase_orders;
VACUUM purchase_orders;

\echo '🎯 Database initialized successfully.'


-- ==========================================================
-- RFM Threshold Table
-- ==========================================================
CREATE TABLE IF NOT EXISTS rfm_thresholds (
    metric TEXT PRIMARY KEY,
    p10 NUMERIC,
    p25 NUMERIC,
    p50 NUMERIC,
    p75 NUMERIC,
    p90 NUMERIC,
    computed_at TIMESTAMPTZ DEFAULT now()
);

\echo '✅ rfm_thresholds table created.'


-- ================================================================
-- Compute percentile-based RFM thresholds — FIXED VERSION
-- (Works in one SQL statement; safe for pgAdmin)
-- ================================================================

WITH base AS (
    SELECT
        product_id,
        DATE_PART('day', now() - MAX(created_date)) AS recency_days,
        COUNT(DISTINCT DATE_TRUNC('month', created_date)) AS frequency_months,
        SUM(net_value)::numeric AS monetary_spend
    FROM purchase_orders
    WHERE product_id IS NOT NULL
    GROUP BY product_id
),
recency_pct AS (
    SELECT
        'recency_days' AS metric,
        percentile_cont(0.10) WITHIN GROUP (ORDER BY recency_days) AS p10,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY recency_days) AS p25,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY recency_days) AS p50,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY recency_days) AS p75,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY recency_days) AS p90
    FROM base
),
frequency_pct AS (
    SELECT
        'frequency_months' AS metric,
        percentile_cont(0.10) WITHIN GROUP (ORDER BY frequency_months) AS p10,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY frequency_months) AS p25,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY frequency_months) AS p50,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY frequency_months) AS p75,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY frequency_months) AS p90
    FROM base
),
monetary_pct AS (
    SELECT
        'monetary_spend' AS metric,
        percentile_cont(0.10) WITHIN GROUP (ORDER BY monetary_spend) AS p10,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY monetary_spend) AS p25,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY monetary_spend) AS p50,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY monetary_spend) AS p75,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY monetary_spend) AS p90
    FROM base
),
all_thresholds AS (
    SELECT * FROM recency_pct
    UNION ALL
    SELECT * FROM frequency_pct
    UNION ALL
    SELECT * FROM monetary_pct
)
INSERT INTO rfm_thresholds (metric, p10, p25, p50, p75, p90, computed_at)
SELECT metric, p10, p25, p50, p75, p90, now()
FROM all_thresholds
ON CONFLICT (metric)
DO UPDATE SET
    p10 = EXCLUDED.p10,
    p25 = EXCLUDED.p25,
    p50 = EXCLUDED.p50,
    p75 = EXCLUDED.p75,
    p90 = EXCLUDED.p90,
    computed_at = now();


-- ==========================================================
-- Materialized View: RFM Score Assignment
-- ==========================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rfm_scores AS
WITH raw AS (
    SELECT
        product_id,
        supplier_id,
        EXTRACT(DAY FROM (NOW() - MAX(created_date)))::NUMERIC AS recency_days,
        COUNT(DISTINCT DATE_TRUNC('month', created_date))::NUMERIC AS frequency_months,
        SUM(net_value)::NUMERIC AS monetary_spend
    FROM purchase_orders
    WHERE supplier_id IS NOT NULL
    GROUP BY product_id, supplier_id
),

r AS (SELECT * FROM rfm_thresholds WHERE metric = 'recency_days'),
f AS (SELECT * FROM rfm_thresholds WHERE metric = 'frequency_months'),
m AS (SELECT * FROM rfm_thresholds WHERE metric = 'monetary_spend')

SELECT
    raw.product_id,
    raw.supplier_id,

    /* =====================
       RECENCY SCORE (R1–R5)
       ===================== */
    CASE
        WHEN raw.recency_days <= r.p10 THEN 5
        WHEN raw.recency_days <= r.p25 THEN 4
        WHEN raw.recency_days <= r.p50 THEN 3
        WHEN raw.recency_days <= r.p75 THEN 2
        ELSE 1
    END AS r_score,

    /* =====================
       FREQUENCY SCORE (F1–F5)
       ===================== */
    CASE
        WHEN raw.frequency_months >= f.p90 THEN 5
        WHEN raw.frequency_months >= f.p75 THEN 4
        WHEN raw.frequency_months >= f.p50 THEN 3
        WHEN raw.frequency_months >= f.p25 THEN 2
        ELSE 1
    END AS f_score,

    /* =====================
       MONETARY SCORE (M1–M5)
       ===================== */
    CASE
        WHEN raw.monetary_spend >= m.p90 THEN 5
        WHEN raw.monetary_spend >= m.p75 THEN 4
        WHEN raw.monetary_spend >= m.p50 THEN 3
        WHEN raw.monetary_spend >= m.p25 THEN 2
        ELSE 1
    END AS m_score

FROM raw, r, f, m;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_rfm_scores
  ON mv_rfm_scores (product_id, supplier_id);

\echo '✅ mv_rfm_scores created (with RFM categories).'

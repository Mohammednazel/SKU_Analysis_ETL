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
-- 2️⃣ ETL Metadata Tables (Checkpoints)
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

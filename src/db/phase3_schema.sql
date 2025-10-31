-- ============================
-- Phase 3: Summary Layer (DDL)
-- ============================

-- 1) SKU-level spend & frequency
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

-- Required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sku_spend_product
  ON mv_sku_spend(product_id);


-- 2) Supplier x Month spend
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


-- 3) Purchasing group (category) spend
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


-- -----------------------------------------
-- (Optional) Snapshot tables for caching /
-- audit of MV states over time (daily).
-- -----------------------------------------
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

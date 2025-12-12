-- step6_db/schema_postgres.sql
-- Run as a superuser / DB owner
-- Created/updated to be compatible with PostgreSQL partitioning rules.

-- Create schema
CREATE SCHEMA IF NOT EXISTS app_core;

-- ----------------------------------------------------------------
-- Headers table (monthly partitioned on order_date)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app_core.purchase_order_headers (
    purchase_order_id     text NOT NULL,
    order_date            timestamptz NOT NULL,
    buyer_company_name    text,
    buyer_email           text,
    supplier_company_name text,
    supplier_id           text,
    subtotal              numeric,
    tax                   numeric,
    grand_amount          numeric,
    currency              text,
    status                text,
    cdate                 timestamptz,
    _raw_json             jsonb,
    PRIMARY KEY (purchase_order_id, order_date)  -- OK: includes partition key
)
PARTITION BY RANGE (order_date);

-- Helpful indexes on headers (non-unique)
CREATE INDEX IF NOT EXISTS idx_headers_order_date
    ON app_core.purchase_order_headers (order_date);

CREATE INDEX IF NOT EXISTS idx_headers_supplier
    ON app_core.purchase_order_headers (supplier_id);

CREATE INDEX IF NOT EXISTS idx_headers_currency
    ON app_core.purchase_order_headers (currency);


-- ----------------------------------------------------------------
-- Items table (monthly partitioned on order_date)
-- ----------------------------------------------------------------
-- NOTE: We CANNOT declare a PRIMARY KEY on the parent that omits the
-- partition key (order_date). To avoid the "unique constraint must
-- include partition key" error, we do NOT declare a PK here.
-- Uniqueness of (purchase_order_id, purchase_order_no) must be enforced
-- by the ETL (recommended) or by creating per-partition unique indexes.
CREATE TABLE IF NOT EXISTS app_core.purchase_order_items (
    purchase_order_id    text NOT NULL,
    purchase_order_no    text NOT NULL,
    item_id              text,
    description          text,
    quantity             numeric,
    unit_of_measure      text,
    unit_price           numeric,
    total                numeric,
    currency             text,
    order_date           timestamptz NOT NULL,
    cdate                timestamptz,
    supplier_id          text,
    plant                text,
    material_group       text,
    product_id           text,
    _raw_json            jsonb
)
PARTITION BY RANGE (order_date);

-- Indexes to support lookups/ETL (non-unique)
CREATE INDEX IF NOT EXISTS idx_items_order_date
    ON app_core.purchase_order_items (order_date);

CREATE INDEX IF NOT EXISTS idx_items_po
    ON app_core.purchase_order_items (purchase_order_id);

CREATE INDEX IF NOT EXISTS idx_items_po_item
    ON app_core.purchase_order_items (purchase_order_id, purchase_order_no);

CREATE INDEX IF NOT EXISTS idx_items_product
    ON app_core.purchase_order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_items_supplier
    ON app_core.purchase_order_items (supplier_id);


-- ----------------------------------------------------------------
-- Quarantine table (keeps rows we could not ingest)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app_core.quarantine_purchase_items (
    id serial PRIMARY KEY,
    purchase_order_id   text,
    purchase_order_no   text,
    reason              text,
    payload             jsonb,
    quarantined_at      timestamptz DEFAULT now()
);


-- ----------------------------------------------------------------
-- Audit table for anomalous conflicts (store minimal info)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app_core.audit_conflicts (
    id serial PRIMARY KEY,
    table_name text NOT NULL,
    pk jsonb NOT NULL,                -- e.g. { "purchase_order_id": "...", "purchase_order_no": "..." }
    existing_row jsonb,
    incoming_row jsonb,
    diff_fields text[],               -- fields that differ
    created_at timestamptz DEFAULT now()
);


-- ----------------------------------------------------------------
-- Audit run metadata
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app_core.audit_runs (
    run_id uuid PRIMARY KEY,
    step text NOT NULL,
    start_time timestamptz,
    end_time timestamptz,
    counters jsonb,
    notes text
);


-- ----------------------------------------------------------------
-- Extensions
-- ----------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ----------------------------------------------------------------
-- Notes / operational recommendations (no-op SQL):
-- ----------------------------------------------------------------
-- 1) If you later decide DB-level uniqueness for items is required,
--    create per-partition UNIQUE INDEX on (purchase_order_id, purchase_order_no)
--    inside the partition creation routine (partition_generator.py).
--
--    Example (inside partition creation):
--    CREATE UNIQUE INDEX IF NOT EXISTS ux_items_p_2024_05_po_item
--      ON app_core.purchase_order_items_p_2024_05 (purchase_order_id, purchase_order_no);
--
-- 2) ETL should deduplicate before staging. ON CONFLICT DO NOTHING can be used
--    on per-partition INSERTs if you have a unique index on the partition.
--
-- 3) After schema is applied:
--    - Run the partition generator to create monthly partitions + per-partition indexes.
--    - Verify partition routing by inserting test rows and checking tableoid.

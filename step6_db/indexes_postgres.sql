-- step6_db/indexes_postgres.sql

-- Indexes on headers (create on parent for convenience)
CREATE INDEX IF NOT EXISTS idx_headers_supplier ON app_core.purchase_order_headers (supplier_company_name);
CREATE INDEX IF NOT EXISTS idx_headers_order_date ON app_core.purchase_order_headers (order_date);

-- Indexes on items (parent)
CREATE INDEX IF NOT EXISTS idx_items_po ON app_core.purchase_order_items (purchase_order_id);
CREATE INDEX IF NOT EXISTS idx_items_itemid ON app_core.purchase_order_items (item_id);
CREATE INDEX IF NOT EXISTS idx_items_material_group ON app_core.purchase_order_items (material_group);
CREATE INDEX IF NOT EXISTS idx_items_order_date ON app_core.purchase_order_items (order_date);
CREATE UNIQUE INDEX IF NOT EXISTS ux_items_po_item ON app_core.purchase_order_items (purchase_order_id, purchase_order_no);

-- Note: Partition-level indexes will be created by partition_generator.py for each partition.

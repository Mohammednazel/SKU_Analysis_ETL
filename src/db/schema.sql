-- src/db/schema.sql
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

-- Indexes for fast filters
CREATE INDEX IF NOT EXISTS idx_po_created_date ON purchase_orders(created_date);
CREATE INDEX IF NOT EXISTS idx_po_supplier_id ON purchase_orders(supplier_id);
CREATE INDEX IF NOT EXISTS idx_po_product_id ON purchase_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_po_material_group ON purchase_orders(material_group);

-- ETL Run Audit Log
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id SERIAL PRIMARY KEY,
    mode TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_processed INT,
    rows_inserted INT,
    rows_updated INT,
    status TEXT,
    error_message TEXT
);

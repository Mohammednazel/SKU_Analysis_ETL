📘 SKU Analysis ETL — End-to-End Documentation (Extraction → Cleaning → Staging → Loading → Audit)
Version: Step-6 (Current), Extraction Phase Not Yet Added
⭐ OVERVIEW

The SKU Analysis ETL pipeline is responsible for:

Extracting SAP Purchase Orders (POs) from an SAP OData API

Cleaning and transforming raw PO JSON into a normalized Header–Item structure

Generating Step-6-ready CSVs that exactly match PostgreSQL staging tables

Ingesting data into PostgreSQL, into monthly partitions

Auditing anomalies, including:

Quantity/price mismatches

Duplicate row differences

Missing Header relationships (quarantine)

🧩 ETL PHASE 1 — EXTRACTION (SAP API)

(You will build this after Step-6; documented here for clarity)

SAP API returns deeply nested Purchase Order data:

Header block

Items array (to_items.results)

Dates encoded as "/Date(####)/"

Mixed numeric formats

Missing fields

Incorrect data types

These are saved as raw JSON snapshots for traceability.

You have not yet built this SAP Extraction script, so audit tables are empty.

🧩 ETL PHASE 2 — CLEANING (Step 1–5)
Step 1 — Raw JSON flattening

Converts SAP nested structure into item-level rows

Each item row gets _header_json embedded

Samples 1 file per run: raw/raw_YYYYMMDD.json

Step 2 — Date parsing

Converts /Date(…) into valid ISO timestamps 2025-04-20T00:00:00+00:00

Adds fields:

order_date_iso

cdate_iso

Step 3 — Quantity & price numeric normalization

Adds:

_quantity_float

_unit_price_float

_total_float

Guarantees all numeric values match PostgreSQL numeric types.

Step 4 — Validation + Quarantine

Any row missing required header relationship (_header_json) is:

Not allowed into final CSV

Added into audit/items_missing_header.csv

Later inserted into quarantine table in DB (optional step)

This protects DB referential integrity.

Step 5.5 — Generate Step-6 Ready CSVs

Produces strict, database-ready CSVs:

Headers CSV → staging_headers_tmp
purchase_order_id
order_date
buyer_company_name
buyer_email
supplier_company_name
supplier_id
subtotal
tax
grand_amount
currency
status
cdate
_raw_json

Items CSV → staging_items_tmp
purchase_order_id
purchase_order_no
item_id
description
quantity
unit_of_measure
unit_price
total
currency
order_date
cdate
supplier_id
plant
material_group
product_id
_raw_json


These CSVs match DB staging tables 1-to-1.

🧩 ETL PHASE 3 — LOADING (Step 6)
Step 6A — Staging Load

Using Python ingestion script:

copy_csv_to_staging()


Loads CSVs into:

app_core.staging_headers_tmp

app_core.staging_items_tmp

Step 6B — Promote Headers

Uses:

promote_headers()


This inserts into:

app_core.purchase_order_headers


with:

ON CONFLICT (purchase_order_id, order_date) DO NOTHING;


Meaning:

Duplicate headers are ignored

No audit is created (headers rarely change historically)

Step 6C — Promote Items (Current Version: No Conflict Logic)

Today the code uses:

INSERT INTO app_core.purchase_order_items (…) VALUES (…)


No ON CONFLICT because partitioned tables cannot enforce unique constraint across all partitions.

Result:

✔ Items insert successfully
✖ No conflict checking
✖ No audit of changed values

👉 This is why your audit tables are EMPTY.

🧩 ETL PHASE 4 — AUDITING (Current vs. Future)

This is the part you're confused about — let's explain in detail.

⭐ WHY ARE audit_conflicts AND audit_runs EMPTY?

Because:

1️⃣ insert_item_with_audit() FUNCTION IS NEVER CALLED

Your ingestion script currently uses:

promote_items(conn)


which inserts directly into:

purchase_order_items


NOT using:

app_core.insert_item_with_audit()


Therefore:

No conflict detection

No mismatch comparison

No audit logs inserted

No audit_run row created

2️⃣ YOU REMOVED ON CONFLICT

This makes conflicts impossible → no audit triggers.

3️⃣ No item-level conflict can occur in a "clean load"

Since you truncated tables, this is a first load, so:

No duplicates

No differences

Therefore no audit events

4️⃣ audit_runs is NEVER inserted into

We did not implement:

INSERT INTO app_core.audit_runs(...)


This will be added only during the production ETL scheduler
# SKU Analysis ETL – Clean Architecture Documentation

## 1. Overview

The **SKU Analysis ETL** system is a production‑grade data pipeline designed to extract Purchase Order data from SAP, clean and normalize it, and ingest it safely into a partitioned PostgreSQL database for analytics and reporting.

The architecture follows **Clean Architecture principles**:

* Clear separation of concerns
* Stateless, repeatable pipeline stages
* Idempotent database writes
* Safe recovery from failures

This document describes **how the system is structured, how components interact, and how daily operations are executed**.

---

## 2. High‑Level Architecture

```
SAP OData API
      │
      ▼
[Extract Layer]
      │
      ▼
[Flattened JSONL Files]
      │
      ▼
[Cleaning & Normalization]
      │
      ▼
[Cleaned JSONL]
      │
      ▼
[CSV Staging]
      │
      ▼
[PostgreSQL (Partitioned)]
```

Each stage is **independent**, **restartable**, and **observable**.

---

## 3. Layered Architecture

### 3.1 Extract Layer (SAP Integration)

**Purpose:** Retrieve raw purchase order data from SAP using time‑bounded queries.

**Location:**

```
extract/sap/
```

**Key Responsibilities:**

* Authenticate with SAP (token handling)
* Fetch paginated OData responses
* Persist raw responses immediately to disk
* Support historical and daily extraction modes

**Key Files:**

* `run_historical_extract.py`
* `run_daily_extract.py`
* `fetch_po_pages.py`

**Design Principles:**

* No transformation logic
* No database interaction
* Disk‑first strategy to protect against API failures

---

### 3.2 Flattening Layer

**Purpose:** Convert nested SAP OData payloads into row‑level records.

**Output Format:**

```
.jsonl (one item per line)
```

**Key Properties:**

* Streaming (low memory usage)
* Append‑only
* Lossless (raw JSON preserved)

This layer ensures **every item becomes an independent record** for downstream processing.

---

### 3.3 Cleaning & Normalization Layer

**Purpose:** Enforce schema correctness and business rules.

**Location:**

```
data_cleaning_dev/
```

**Key Scripts:**

* `cleaning_step4_date_numeric.py`

**Responsibilities:**

* SAP date parsing (`/Date(ms)/`, ISO formats)
* Numeric normalization
* Missing field handling
* Quarantine of invalid rows
* Creation of canonical fields:

  * `order_date_iso`
  * `cdate_iso`
  * `_quantity_float`, `_unit_price_float`, `_total_float`

**Key Design Decision:**

* Output remains **JSONL** to preserve flexibility and auditability

---

### 3.4 Staging / CSV Preparation Layer

**Purpose:** Prepare data for PostgreSQL COPY ingestion safely.

**Key Script:**

* `cleaning_step5_5_generate_step6_ready_csvs.py`

**Outputs:**

```
staging/step6_headers.csv
staging/step6_items.csv
```

**Critical Guarantees:**

* Strict column ordering
* `csv.QUOTE_ALL` enforced
* Newlines stripped from JSON
* One row = one CSV line (no column shifting)

**Why CSV?**

* Fast bulk ingestion
* Predictable performance
* Clear schema enforcement

---

### 3.5 Database Ingestion Layer

**Purpose:** Load data safely into PostgreSQL.

**Location:**

```
step6_db/
```

**Key Files:**

* `schema_postgres.sql`
* `partition_generator.py`
* `ingest_from_csv.py`

#### Database Design

* **Partitioned by `order_date` (monthly)**
* Separate tables for:

  * `purchase_order_headers`
  * `purchase_order_items`

#### Safety Guarantees

* Idempotent inserts (`WHERE NOT EXISTS`)
* Duplicate protection on reruns
* Retry‑safe historical loads

#### Ingestion Flow

1. Load CSV → staging tables
2. Promote headers
3. Promote items safely
4. Cleanup staging tables

---

## 4. Daily Automation Flow

### 4.1 Daily Execution Logic

* Fetches **yesterday’s data only**
* Handles "no data" days gracefully
* Does not create empty files

### 4.2 Daily Runbook

```
1. Extract
   python -m extract.sap.run_daily_extract

2. Clean
   python cleaning_step4_date_numeric.py --input daily.jsonl --output cleaned_daily.jsonl

3. Stage
   python cleaning_step5_5_generate_step6_ready_csvs.py --input cleaned_daily.jsonl

4. Ingest
   python ingest_from_csv.py step6_headers.csv step6_items.csv
```

**Result:**

* New rows inserted
* Duplicates skipped
* Safe to re‑run

---

## 5. Historical vs Daily Strategy

| Aspect       | Historical       | Daily     |
| ------------ | ---------------- | --------- |
| Data Volume  | Large            | Small     |
| Disk Usage   | High             | Minimal   |
| Execution    | Manual / Chunked | Automated |
| Retry Safety | Required         | Built‑in  |

**Current State:**

* Historical: Jan 2024 loaded
* Daily: Fully automated, awaiting client API readiness

---

## 6. Operational Guarantees

✔ Stateless pipeline stages
✔ Safe retries
✔ No duplicate inserts
✔ Partition‑safe writes
✔ Disk‑aware strategy
✔ Audit & quarantine support

---

## 7. Failure Scenarios & Recovery

| Scenario              | Impact         | Recovery           |
| --------------------- | -------------- | ------------------ |
| API returns no data   | None           | Skip day           |
| Script crashes        | Partial output | Re‑run safely      |
| DB connection failure | No ingestion   | Re‑run Step 6      |
| Duplicate run         | None           | Duplicates skipped |

---

## 8. Future Extensions (Optional)

* Azure Scheduler / Functions
* Incremental historical backfill controller
* Data quality dashboards
* Downstream analytics marts

---

## 9. Final Notes

This architecture is **production‑ready** and **client‑safe**. No further structural changes are required. Any future work is operational or scaling‑related, not foundational.

---

**Document Version:** 1.0
**Status:** Approved for Production

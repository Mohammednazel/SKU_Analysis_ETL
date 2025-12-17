#!/bin/bash
set -e  # Exit immediately if any command fails

echo "🚀 Starting Daily SKU ETL Pipeline..."

# Use /tmp for intermediate files to avoid permission issues
WORKDIR="/tmp"

echo "📦 Step 1: Extracting (SAP -> JSONL)..."
# Explicitly output to /tmp so we know exactly where the file is
python -m extract.sap.run_daily_extract --output ${WORKDIR}/daily.jsonl

echo "🧹 Step 2: Cleaning (JSONL -> JSONL)..."
python data_cleaning_dev/cleaning_step4_date_numeric.py \
    --input ${WORKDIR}/daily.jsonl \
    --output ${WORKDIR}/cleaned.jsonl

echo "📄 Step 3: Staging (JSONL -> CSV)..."
python data_cleaning_dev/cleaning_step5_5_generate_step6_ready_csvs.py \
    --input ${WORKDIR}/cleaned.jsonl \
    --output_dir ${WORKDIR}

echo "📥 Step 4: Ingesting (CSV -> Postgres)..."
# Ingest expects headers first, then items
python step6_db/ingest_from_csv.py \
    ${WORKDIR}/step6_headers.csv \
    ${WORKDIR}/step6_items.csv

echo "🔄 Step 5: Refreshing Analytics MVs..."
python analysis/sql/refresh/run_refresh_daily.py

echo "✅ Daily ETL Pipeline Complete."
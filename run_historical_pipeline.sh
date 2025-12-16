#!/bin/bash
set -e

echo "🚀 Starting HISTORICAL Load..."
WORKDIR="/tmp"

# Step 1: Extract HISTORY
# Now this --output flag will actually work!
echo "📦 Step 1: Extracting Historical Data..."
python -m extract.sap.run_historical_extract --output ${WORKDIR}/history.jsonl

# Step 2: Clean
echo "🧹 Step 2: Cleaning..."
python data_cleaning_dev/cleaning_step4_date_numeric.py \
    --input ${WORKDIR}/history.jsonl \
    --output ${WORKDIR}/cleaned.jsonl

# Step 3: Stage
echo "📄 Step 3: Staging CSVs..."
python data_cleaning_dev/cleaning_step5_5_generate_step6_ready_csvs.py \
    --input ${WORKDIR}/cleaned.jsonl \
    --output_dir ${WORKDIR}

# Step 4: Ingest
echo "📥 Step 4: Ingesting to Azure DB..."
python step6_db/ingest_from_csv.py \
    ${WORKDIR}/step6_headers.csv \
    ${WORKDIR}/step6_items.csv

# Step 5: Refresh
echo "🔄 Step 5: Refreshing Views..."
python analysis/sql/refresh/run_refresh_daily.py

echo "✅ Historical Load Complete."
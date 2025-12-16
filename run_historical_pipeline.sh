#!/bin/bash
echo "🚀 Starting HISTORICAL Load..."

# CRITICAL FIX: Tell Python where the root 'extract' folder lives
export PYTHONPATH=/app

# Run the smart python script
python /app/extract/sap/run_historical_extract.py

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "✅ Historical Pipeline Completed Successfully."
else
    echo "❌ Historical Pipeline Failed."
fi

exit $exit_code
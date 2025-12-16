#!/bin/bash
echo "🚀 Starting HISTORICAL Load..."

# Run the smart python script
# It handles Extract -> Transform -> Load internally now.
python /app/extract/sap/run_historical_extract.py

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "✅ Historical Pipeline Completed Successfully."
else
    echo "❌ Historical Pipeline Failed."
fi

exit $exit_code
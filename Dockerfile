FROM python:3.11-slim

# Prevent Python buffering logs (important for Azure logs)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps for psycopg2
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy full project
COPY . .

# Ensure pipeline script is executable
RUN chmod +x run_daily_pipeline.sh

# Default command = API
# (ACA Job will override this with run_daily_pipeline.sh)
CMD ["python", "-m", "uvicorn", "analysis.api.main:app", "--host", "0.0.0.0", "--port", "80"]

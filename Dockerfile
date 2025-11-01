# ---------- Base image ----------
FROM python:3.11-slim AS base
WORKDIR /app

# ---------- Install system dependencies ----------
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# ---------- Copy and install Python dependencies ----------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Copy your application ----------
COPY . .

# ---------- Environment ----------
ENV PYTHONPATH=/app/src \
    TZ=UTC

# ---------- Default command ----------
CMD ["python", "run_api.py"]

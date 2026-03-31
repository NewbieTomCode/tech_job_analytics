FROM python:3.13-slim

WORKDIR /app

# Install system dependencies for psycopg
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY job-intelligence/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY job-intelligence/ .

CMD ["python", "run_pipeline.py"]

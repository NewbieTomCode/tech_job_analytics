FROM python:3.13-slim

WORKDIR /opt/airflow

# Install system dependencies for psycopg and Airflow
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY job-intelligence/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY job-intelligence/src/ ./src/
COPY job-intelligence/run_pipeline.py .
COPY job-intelligence/conftest.py .
COPY dags/ ./dags/

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"

CMD ["python", "run_pipeline.py"]

# tech_job_analytics

🚀 Project Overview

This project builds an end-to-end data engineering pipeline to:

Collect job listings from the Adzuna API

Store and deduplicate job data

Track newly posted jobs

Extract and analyze required skills

Generate market insights

Run on AWS EC2

The system is designed to simulate a real-world data platform used in industry.

🏗️ Architecture
Adzuna API
     ↓
Data Ingestion (Python)
     ↓
PostgreSQL Database (raw_jobs staging → cleaned jobs)
     ↓
Data Processing (cleaning, dedup, skill extraction)
     ↓
Analytics & Streamlit Dashboard
     ↓
User Reports

Orchestrated by Apache Airflow (DAG: fetch → load_raw → clean_and_upsert → extract_skills → log_run)

Deployed on: AWS EC2 (Ubuntu Linux) via Docker Compose

📊 Features
✅ Data Collection

Fetches graduate and internship jobs using Adzuna API

Supports pagination and filtering

Handles API rate limits

✅ Data Storage

Stores job listings in PostgreSQL

Deduplicates using URL hashing

Tracks scrape timestamps

✅ New Job Detection

Identifies newly listed jobs

Stores historical records

Enables daily/weekly reporting

✅ Skill Extraction

Extracts technical skills from job descriptions

Supports multi-word skills (e.g. “machine learning”)

Saves structured skill data

✅ Analytics

Most in-demand skills

Top hiring companies

Job trends over time

Location-based insights

✅ Orchestration & Automation

Apache Airflow DAG with task-level retries

Scheduled daily data ingestion (8am UTC)

Airflow web UI for monitoring at localhost:8080

Logging and error handling

📁 Project Structure
tech_job_analytics/
│
├── Dockerfile
├── docker-compose.yml               # Postgres + Airflow + app services
├── init-airflow-db.sql              # Creates Airflow metadata database
├── README.md
├── .gitignore
│
├── dags/
│   └── job_pipeline_dag.py          # Airflow DAG: orchestrates the ELT pipeline
│
└── job-intelligence/
    ├── run_pipeline.py              # CLI entry point (manual runs)
    ├── conftest.py                  # pytest path configuration
    ├── requirements.txt
    │
    ├── src/
    │   ├── extract/
    │   │   └── fetch_data.py        # Adzuna API client with pagination
    │   │
    │   ├── transform/
    │   │   ├── clean_data.py        # Data validation & normalization
    │   │   └── skill_extractor.py   # NLP-based skill extraction
    │   │
    │   ├── load/
    │   │   └── load_data.py         # Raw insert + dedup upsert into Postgres
    │   │
    │   ├── database_connections/
    │   │   ├── schema.sql           # PostgreSQL table definitions
    │   │   └── db_utils.py          # Connection pooling & CRUD operations
    │   │
    │   ├── analytics/
    │   │   └── reports.py           # SQL queries for market insights
    │   │
    │   ├── dashboard/
    │   │   └── app.py               # Streamlit web dashboard
    │   │
    │   └── config/
    │       ├── config.yaml          # Pipeline configuration
    │       └── settings.py          # Environment variable loader
    │
    ├── scheduler/
    │   └── cron_jobs.sh             # Cron alternative (if not using Airflow)
    │
    └── tests/
        ├── test_extract.py          # API ingestion tests
        ├── test_clean_data.py       # Data cleaning tests
        ├── test_skill_extractor.py  # Skill extraction tests
        ├── test_db_utils.py         # Database utility tests
        ├── test_load.py             # Load pipeline tests
        └── test_reports.py          # Analytics query tests

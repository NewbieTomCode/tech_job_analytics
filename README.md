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
PostgreSQL Database
     ↓
Data Processing (Pandas / NLP)
     ↓
Analytics & Dashboard
     ↓
User Reports

Deployed on:

AWS EC2 (Ubuntu Linux)

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

✅ Automation

Scheduled daily data ingestion

Automatic updates

Logging and error handling

📁 Project Structure
job-intelligence/
│
├── ingestion/
│   └── adzuna_client.py
│
├── database/
│   ├── schema.sql
│   └── db_utils.py
│
├── processing/
│   ├── clean_data.py
│   └── skill_extractor.py
│
├── analytics/
│   └── reports.py
│
├── dashboard/
│   └── app.py
│
├── scheduler/
│   └── cron_jobs.sh
│
├── config/
│   └── config.yaml
│
├── requirements.txt
└── README.md

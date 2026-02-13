# Adzuna API client for job data ingestion
import math
import json
import requests
from src.config.settings import ADZUNA_APP_KEY, ADZUNA_APP_ID

if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
    raise ValueError("Missing API credentials")

def fetch_job_data():
    url = f"https://api.adzuna.com/v1/api/jobs/gb/search/1"
    results_per_page = 50

    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "full_time": 1,
        "permanent": 1,
        "where": "London",
        "what_or": "tech,dataengineer,software",
        "results_per_page": results_per_page,
    }

    response = requests.get(url, params=params)
    data = response.json()
    total_pages = math.ceil(data["count"] / results_per_page)

    all_jobs = []

    for page in range(1, total_pages + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/gb/search/{page}"
        response = requests.get(url, params=params)
        data = response.json()
        all_jobs.extend(data["results"])
        print(f"Fetched page {page}")
    print(all_jobs)

fetch_job_data()
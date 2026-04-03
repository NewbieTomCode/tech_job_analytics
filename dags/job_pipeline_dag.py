"""
Job Intelligence Pipeline DAG

Two parallel branches after fetch, then warehouse sync:

  Branch 1 (raw staging):   fetch_jobs → load_raw ──────────────────┐
  Branch 2 (cleaned ELT):   fetch_jobs → clean_and_upsert           │
                               → extract_skills → log_pipeline_run ─┤
                                                                    └→ sync_warehouse

Runs daily at 8am UTC. Each task is independent and retryable.
"""
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args – apply to every task in the DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "job_intelligence",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Task callables – thin wrappers around existing src/ functions
# ---------------------------------------------------------------------------

def _fetch_jobs(**context):
    """Extract: fetch job listings from Adzuna API."""
    from src.extract.fetch_data import fetch_job_data

    jobs = fetch_job_data()
    # Push to XCom so downstream tasks can pull the data
    context["ti"].xcom_push(key="raw_jobs", value=jobs)
    logger.info(f"Fetched {len(jobs)} jobs from API")
    return len(jobs)


# -- Branch 1: Raw staging -------------------------------------------------

def _load_raw(**context):
    """Load: insert raw API responses into the staging table."""
    from src.database_connections.db_utils import insert_raw_jobs

    jobs = context["ti"].xcom_pull(task_ids="fetch_jobs", key="raw_jobs")
    count = insert_raw_jobs(jobs)
    logger.info(f"Loaded {count} raw records into staging")
    return count


# -- Branch 2: Clean → Extract Skills → Log --------------------------------

def _clean_and_upsert(**context):
    """Transform + Load: validate, clean data, deduplicate, upsert into jobs table."""
    from src.transform.clean_data import clean_job_batch
    from src.database_connections.db_utils import upsert_jobs

    jobs = context["ti"].xcom_pull(task_ids="fetch_jobs", key="raw_jobs")

    # Clean with two-stage Pydantic validation (raw → clean → validate)
    cleaned = clean_job_batch(jobs)
    rejected = len(jobs) - len(cleaned)
    logger.info(
        f"Validation + cleaning: {len(cleaned)} valid from {len(jobs)} raw "
        f"({rejected} rejected)"
    )

    # Upsert into database
    stats = upsert_jobs(cleaned)
    stats["validation_rejected"] = rejected
    context["ti"].xcom_push(key="upsert_stats", value=stats)
    context["ti"].xcom_push(key="cleaned_jobs", value=cleaned)
    logger.info(f"Upsert: {stats['new']} new, {stats['duplicates']} duplicates")
    return stats


def _extract_skills(**context):
    """Transform: extract skills from cleaned job descriptions."""
    from src.transform.skill_extractor import extract_skills_batch, get_top_skills

    cleaned = context["ti"].xcom_pull(task_ids="clean_and_upsert", key="cleaned_jobs")
    result = extract_skills_batch(cleaned)

    top = get_top_skills(result["skill_counts"], top_n=10)
    logger.info(f"Top 10 skills: {top}")

    context["ti"].xcom_push(key="skill_counts", value=result["skill_counts"])
    return len(result["skill_counts"])


def _log_pipeline_run(**context):
    """Log: record pipeline run metadata in scrape_runs table."""
    from src.database_connections.db_utils import log_scrape_run

    stats = context["ti"].xcom_pull(task_ids="clean_and_upsert", key="upsert_stats")
    jobs = context["ti"].xcom_pull(task_ids="fetch_jobs", key="raw_jobs")
    fetched_count = len(jobs) if jobs else 0

    log_scrape_run(
        jobs_fetched=fetched_count,
        new_jobs=stats.get("new", 0),
        duplicates=stats.get("duplicates", 0),
        status="success",
    )
    logger.info("Pipeline run logged successfully")


def _sync_warehouse(**context):
    """Sync cleaned data from PostgreSQL into DuckDB analytical warehouse."""
    from src.warehouse.duckdb_loader import sync_all

    summary = sync_all()
    logger.info(f"Warehouse sync: {summary}")
    return summary


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="job_intelligence_pipeline",
    default_args=default_args,
    description="ELT pipeline: Adzuna API → PostgreSQL → Analytics",
    schedule_interval="0 8 * * *",  # daily at 8am UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["job-intelligence", "elt"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_jobs",
        python_callable=_fetch_jobs,
    )

    # Branch 1: raw staging
    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=_load_raw,
    )

    # Branch 2: clean → skills → log
    clean_upsert = PythonOperator(
        task_id="clean_and_upsert",
        python_callable=_clean_and_upsert,
    )

    skills = PythonOperator(
        task_id="extract_skills",
        python_callable=_extract_skills,
    )

    log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=_log_pipeline_run,
    )

    # Warehouse sync — runs after both branches complete
    warehouse_sync = PythonOperator(
        task_id="sync_warehouse",
        python_callable=_sync_warehouse,
    )

    # Task dependencies — two branches from fetch, then converge at warehouse
    #
    #              ┌→ load_raw ──────────────────────────────┐
    # fetch_jobs ──┤                                        ├→ sync_warehouse
    #              └→ clean_and_upsert → extract_skills → log_pipeline_run ─┘
    #
    fetch >> load_raw
    fetch >> clean_upsert >> skills >> log_run
    [load_raw, log_run] >> warehouse_sync

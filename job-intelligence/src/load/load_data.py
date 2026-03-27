# Load step: takes fetched job data and writes it to PostgreSQL
import json
import logging

from src.database_connections.db_utils import insert_raw_jobs, upsert_jobs, log_scrape_run

logger = logging.getLogger(__name__)


def load_jobs(job_list: list) -> dict:
    """
    Full load pipeline:
    1. Insert raw API response into staging table (raw_jobs)
    2. Deduplicate and upsert into cleaned jobs table
    3. Log the scrape run metadata

    Returns summary stats dict.
    """
    if not job_list:
        logger.warning("No jobs to load")
        return {"fetched": 0, "new": 0, "duplicates": 0}

    # Step 1: Store raw data
    raw_count = insert_raw_jobs(job_list)
    logger.info(f"Raw layer: {raw_count} records staged")

    # Step 2: Deduplicate and insert into cleaned table
    stats = upsert_jobs(job_list)
    logger.info(f"Cleaned layer: {stats['new']} new, {stats['duplicates']} duplicates")

    # Step 3: Log the run
    log_scrape_run(
        jobs_fetched=raw_count,
        new_jobs=stats["new"],
        duplicates=stats["duplicates"],
    )

    return {
        "fetched": raw_count,
        "new": stats["new"],
        "duplicates": stats["duplicates"],
    }


def load_from_json(filepath: str) -> dict:
    """Load jobs from a previously saved JSON file into the database."""
    with open(filepath, "r") as f:
        job_list = json.load(f)

    logger.info(f"Loaded {len(job_list)} jobs from {filepath}")
    return load_jobs(job_list)

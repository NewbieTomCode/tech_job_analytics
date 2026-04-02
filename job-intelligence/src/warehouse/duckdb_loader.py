# DuckDB analytical warehouse layer
# Exports cleaned data from PostgreSQL into DuckDB for fast OLAP queries.
# Local dev replacement for BigQuery — same columnar benefits, zero infra.

import os
import logging
from datetime import datetime

import duckdb
from sqlalchemy import text

from src.database_connections.db_utils import get_session

logger = logging.getLogger(__name__)

# Default warehouse path — can be overridden via env var
WAREHOUSE_PATH = os.getenv("DUCKDB_PATH", "warehouse/job_analytics.duckdb")


def get_warehouse_connection(path: str = None) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. Creates the file + dirs if they don't exist."""
    db_path = path or WAREHOUSE_PATH
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    return duckdb.connect(db_path)


def init_warehouse(con: duckdb.DuckDBPyConnection = None) -> None:
    """Create analytical tables in DuckDB if they don't exist."""
    close_after = con is None
    con = con or get_warehouse_connection()

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_jobs (
            job_id          INTEGER PRIMARY KEY,
            adzuna_id       VARCHAR,
            url             VARCHAR,
            title           VARCHAR,
            company         VARCHAR,
            location        VARCHAR,
            description     VARCHAR,
            salary_min      DOUBLE,
            salary_max      DOUBLE,
            contract_type   VARCHAR,
            category        VARCHAR,
            posted_date     TIMESTAMP,
            first_seen_at   TIMESTAMP,
            last_seen_at    TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_skills (
            skill_id    INTEGER PRIMARY KEY,
            skill_name  VARCHAR UNIQUE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_job_skills (
            job_id      INTEGER,
            skill_id    INTEGER,
            PRIMARY KEY (job_id, skill_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_pipeline_runs (
            run_id          INTEGER PRIMARY KEY,
            run_date        TIMESTAMP,
            jobs_fetched    INTEGER,
            new_jobs        INTEGER,
            duplicates      INTEGER,
            status          VARCHAR,
            error_message   VARCHAR
        )
    """)

    logger.info("DuckDB warehouse tables initialised")
    if close_after:
        con.close()


def sync_jobs(con: duckdb.DuckDBPyConnection = None) -> int:
    """
    Export cleaned jobs from PostgreSQL into DuckDB dim_jobs.
    Uses a full-refresh strategy: truncate and reload.
    Returns number of rows loaded.
    """
    close_after = con is None
    con = con or get_warehouse_connection()
    init_warehouse(con)

    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT id, adzuna_id, url, title, company, location,
                       description, salary_min, salary_max, contract_type,
                       category, posted_date, first_seen_at, last_seen_at
                FROM jobs
                ORDER BY id
            """)
        ).fetchall()

    if not rows:
        logger.warning("No jobs to sync to warehouse")
        if close_after:
            con.close()
        return 0

    # Full refresh
    con.execute("DELETE FROM dim_jobs")
    con.executemany(
        """INSERT INTO dim_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [tuple(r) for r in rows],
    )
    count = len(rows)
    logger.info(f"Synced {count} jobs to DuckDB warehouse")

    if close_after:
        con.close()
    return count


def sync_skills(con: duckdb.DuckDBPyConnection = None) -> int:
    """Export skills and job_skills from PostgreSQL into DuckDB."""
    close_after = con is None
    con = con or get_warehouse_connection()
    init_warehouse(con)

    with get_session() as session:
        skills = session.execute(
            text("SELECT id, skill_name FROM skills ORDER BY id")
        ).fetchall()

        job_skills = session.execute(
            text("SELECT job_id, skill_id FROM job_skills ORDER BY job_id, skill_id")
        ).fetchall()

    con.execute("DELETE FROM dim_skills")
    if skills:
        con.executemany(
            "INSERT INTO dim_skills VALUES (?, ?)",
            [tuple(r) for r in skills],
        )

    con.execute("DELETE FROM fact_job_skills")
    if job_skills:
        con.executemany(
            "INSERT INTO fact_job_skills VALUES (?, ?)",
            [tuple(r) for r in job_skills],
        )

    logger.info(f"Synced {len(skills)} skills, {len(job_skills)} job-skill links")
    if close_after:
        con.close()
    return len(skills)


def sync_pipeline_runs(con: duckdb.DuckDBPyConnection = None) -> int:
    """Export scrape_runs from PostgreSQL into DuckDB."""
    close_after = con is None
    con = con or get_warehouse_connection()
    init_warehouse(con)

    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT id, run_date, jobs_fetched, new_jobs, duplicates, status, error_message
                FROM scrape_runs
                ORDER BY id
            """)
        ).fetchall()

    con.execute("DELETE FROM fact_pipeline_runs")
    if rows:
        con.executemany(
            "INSERT INTO fact_pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [tuple(r) for r in rows],
        )

    logger.info(f"Synced {len(rows)} pipeline runs to warehouse")
    if close_after:
        con.close()
    return len(rows)


def sync_all(con: duckdb.DuckDBPyConnection = None) -> dict:
    """Full warehouse sync — jobs, skills, and pipeline runs."""
    close_after = con is None
    con = con or get_warehouse_connection()

    jobs_count = sync_jobs(con)
    skills_count = sync_skills(con)
    runs_count = sync_pipeline_runs(con)

    summary = {
        "jobs": jobs_count,
        "skills": skills_count,
        "pipeline_runs": runs_count,
        "synced_at": datetime.utcnow().isoformat(),
    }
    logger.info(f"Warehouse sync complete: {summary}")

    if close_after:
        con.close()
    return summary

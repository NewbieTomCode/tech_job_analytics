# DuckDB analytical warehouse layer
# Exports cleaned data from PostgreSQL into DuckDB for fast OLAP queries.
# Local dev replacement for BigQuery — same columnar benefits, zero infra.
#
# Supports two sync strategies:
#   - Incremental (default): uses a last_sync_at watermark to only pull
#     rows changed since the last sync. New rows are inserted, existing
#     rows are updated via DELETE + INSERT (upsert pattern in DuckDB).
#   - Full refresh: truncate and reload everything. Used as fallback or
#     for initial loads via force_full_refresh=True.

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

    # Watermark table — tracks when each table was last synced
    con.execute("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            table_name      VARCHAR PRIMARY KEY,
            last_sync_at    TIMESTAMP NOT NULL,
            rows_synced     INTEGER DEFAULT 0
        )
    """)

    logger.info("DuckDB warehouse tables initialised")
    if close_after:
        con.close()


# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------
def _get_last_sync(con: duckdb.DuckDBPyConnection, table_name: str) -> datetime | None:
    """Get the last sync timestamp for a given table."""
    row = con.execute(
        "SELECT last_sync_at FROM sync_metadata WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return row[0] if row else None


def _update_sync_metadata(con: duckdb.DuckDBPyConnection, table_name: str,
                          rows_synced: int) -> None:
    """Update or insert the sync watermark for a table."""
    now = datetime.utcnow()
    existing = con.execute(
        "SELECT 1 FROM sync_metadata WHERE table_name = ?",
        [table_name],
    ).fetchone()

    if existing:
        con.execute(
            "UPDATE sync_metadata SET last_sync_at = ?, rows_synced = ? WHERE table_name = ?",
            [now, rows_synced, table_name],
        )
    else:
        con.execute(
            "INSERT INTO sync_metadata VALUES (?, ?, ?)",
            [table_name, now, rows_synced],
        )


# ---------------------------------------------------------------------------
# Sync jobs — incremental by default
# ---------------------------------------------------------------------------
def sync_jobs(con: duckdb.DuckDBPyConnection = None,
              force_full_refresh: bool = False) -> int:
    """
    Sync cleaned jobs from PostgreSQL into DuckDB dim_jobs.

    Incremental mode (default):
        Uses last_seen_at as the watermark column. Only pulls rows from
        Postgres where last_seen_at > last_sync_at. New rows are inserted,
        existing rows are updated (DELETE + INSERT upsert).

    Full refresh mode (force_full_refresh=True):
        Truncates dim_jobs and reloads everything from Postgres.
        Used for initial load or to recover from sync drift.

    Returns number of rows synced in this run.
    """
    close_after = con is None
    con = con or get_warehouse_connection()
    init_warehouse(con)

    last_sync = _get_last_sync(con, "dim_jobs")

    # Decide strategy
    if force_full_refresh or last_sync is None:
        strategy = "full_refresh"
        logger.info("sync_jobs: using full refresh"
                     + (" (forced)" if force_full_refresh else " (first sync)"))
    else:
        strategy = "incremental"
        logger.info(f"sync_jobs: incremental since {last_sync.isoformat()}")

    # Query Postgres
    with get_session() as session:
        if strategy == "incremental":
            rows = session.execute(
                text("""
                    SELECT id, adzuna_id, url, title, company, location,
                           description, salary_min, salary_max, contract_type,
                           category, posted_date, first_seen_at, last_seen_at
                    FROM jobs
                    WHERE last_seen_at > :watermark
                    ORDER BY id
                """),
                {"watermark": last_sync},
            ).fetchall()
        else:
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
        logger.info("sync_jobs: no new/updated rows to sync")
        _update_sync_metadata(con, "dim_jobs", 0)
        if close_after:
            con.close()
        return 0

    if strategy == "full_refresh":
        con.execute("DELETE FROM dim_jobs")
        con.executemany(
            "INSERT INTO dim_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(r) for r in rows],
        )
    else:
        # Incremental upsert: remove stale versions, then insert fresh ones
        job_ids = [r[0] for r in rows]
        # DuckDB supports IN with list parameter
        for job_id in job_ids:
            con.execute("DELETE FROM dim_jobs WHERE job_id = ?", [job_id])
        con.executemany(
            "INSERT INTO dim_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(r) for r in rows],
        )

    count = len(rows)
    _update_sync_metadata(con, "dim_jobs", count)
    logger.info(f"sync_jobs [{strategy}]: synced {count} rows")

    if close_after:
        con.close()
    return count


# ---------------------------------------------------------------------------
# Sync skills — full refresh (small reference table)
# ---------------------------------------------------------------------------
def sync_skills(con: duckdb.DuckDBPyConnection = None) -> int:
    """
    Export skills and job_skills from PostgreSQL into DuckDB.
    Always full refresh — these are small reference tables where
    incremental logic adds complexity without meaningful benefit.
    """
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

    _update_sync_metadata(con, "dim_skills", len(skills))
    logger.info(f"Synced {len(skills)} skills, {len(job_skills)} job-skill links")
    if close_after:
        con.close()
    return len(skills)


# ---------------------------------------------------------------------------
# Sync pipeline runs — incremental by run_date
# ---------------------------------------------------------------------------
def sync_pipeline_runs(con: duckdb.DuckDBPyConnection = None,
                       force_full_refresh: bool = False) -> int:
    """
    Sync scrape_runs from PostgreSQL into DuckDB.
    Incremental by default using run_date as watermark.
    Pipeline runs are append-only so we just insert new ones.
    """
    close_after = con is None
    con = con or get_warehouse_connection()
    init_warehouse(con)

    last_sync = _get_last_sync(con, "fact_pipeline_runs")

    if force_full_refresh or last_sync is None:
        strategy = "full_refresh"
    else:
        strategy = "incremental"

    with get_session() as session:
        if strategy == "incremental":
            rows = session.execute(
                text("""
                    SELECT id, run_date, jobs_fetched, new_jobs, duplicates, status, error_message
                    FROM scrape_runs
                    WHERE run_date > :watermark
                    ORDER BY id
                """),
                {"watermark": last_sync},
            ).fetchall()
        else:
            rows = session.execute(
                text("""
                    SELECT id, run_date, jobs_fetched, new_jobs, duplicates, status, error_message
                    FROM scrape_runs
                    ORDER BY id
                """)
            ).fetchall()

    if strategy == "full_refresh":
        con.execute("DELETE FROM fact_pipeline_runs")

    if rows:
        if strategy == "incremental":
            # Pipeline runs are append-only, but guard against duplicates
            for row in rows:
                con.execute("DELETE FROM fact_pipeline_runs WHERE run_id = ?", [row[0]])
        con.executemany(
            "INSERT INTO fact_pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [tuple(r) for r in rows],
        )

    count = len(rows)
    _update_sync_metadata(con, "fact_pipeline_runs", count)
    logger.info(f"sync_pipeline_runs [{strategy}]: synced {count} rows")

    if close_after:
        con.close()
    return count


# ---------------------------------------------------------------------------
# Sync all
# ---------------------------------------------------------------------------
def sync_all(con: duckdb.DuckDBPyConnection = None,
             force_full_refresh: bool = False) -> dict:
    """
    Full warehouse sync — jobs, skills, and pipeline runs.
    Pass force_full_refresh=True to ignore watermarks and reload everything.
    """
    close_after = con is None
    con = con or get_warehouse_connection()

    jobs_count = sync_jobs(con, force_full_refresh=force_full_refresh)
    skills_count = sync_skills(con)  # always full refresh (small table)
    runs_count = sync_pipeline_runs(con, force_full_refresh=force_full_refresh)

    summary = {
        "jobs": jobs_count,
        "skills": skills_count,
        "pipeline_runs": runs_count,
        "synced_at": datetime.utcnow().isoformat(),
        "strategy": "full_refresh" if force_full_refresh else "incremental",
    }
    logger.info(f"Warehouse sync complete: {summary}")

    if close_after:
        con.close()
    return summary

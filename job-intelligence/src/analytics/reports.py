# Analytics and reporting queries
import logging
from sqlalchemy import text

from src.database_connections.db_utils import get_session

logger = logging.getLogger(__name__)


def get_top_companies(limit: int = 10) -> list:
    """Return companies with the most job listings."""
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT company, COUNT(*) as job_count
                FROM jobs
                WHERE company IS NOT NULL
                GROUP BY company
                ORDER BY job_count DESC
                LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()
        return [{"company": r[0], "job_count": r[1]} for r in rows]


def get_jobs_by_location(limit: int = 10) -> list:
    """Return locations with the most job listings."""
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT location, COUNT(*) as job_count
                FROM jobs
                WHERE location IS NOT NULL
                GROUP BY location
                ORDER BY job_count DESC
                LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()
        return [{"location": r[0], "job_count": r[1]} for r in rows]


def get_salary_stats() -> dict:
    """Return aggregate salary statistics across all jobs with salary data."""
    with get_session() as session:
        row = session.execute(
            text("""
                SELECT
                    COUNT(*) as total_with_salary,
                    ROUND(AVG(salary_min), 2) as avg_min,
                    ROUND(AVG(salary_max), 2) as avg_max,
                    ROUND(MIN(salary_min), 2) as lowest_min,
                    ROUND(MAX(salary_max), 2) as highest_max
                FROM jobs
                WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL
            """)
        ).fetchone()
        return {
            "total_with_salary": row[0],
            "avg_min": float(row[1]) if row[1] else None,
            "avg_max": float(row[2]) if row[2] else None,
            "lowest_min": float(row[3]) if row[3] else None,
            "highest_max": float(row[4]) if row[4] else None,
        }


def get_salary_by_category() -> list:
    """Return average salary range grouped by job category."""
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT
                    category,
                    COUNT(*) as job_count,
                    ROUND(AVG(salary_min), 2) as avg_min,
                    ROUND(AVG(salary_max), 2) as avg_max
                FROM jobs
                WHERE category IS NOT NULL
                    AND salary_min IS NOT NULL
                    AND salary_max IS NOT NULL
                GROUP BY category
                ORDER BY avg_max DESC
            """)
        ).fetchall()
        return [
            {
                "category": r[0],
                "job_count": r[1],
                "avg_min": float(r[2]),
                "avg_max": float(r[3]),
            }
            for r in rows
        ]


def get_jobs_over_time(interval: str = "day") -> list:
    """
    Return job posting counts over time.
    interval: 'day', 'week', or 'month'
    """
    trunc_map = {"day": "day", "week": "week", "month": "month"}
    trunc = trunc_map.get(interval, "day")

    with get_session() as session:
        rows = session.execute(
            text(f"""
                SELECT
                    DATE_TRUNC(:trunc, posted_date) as period,
                    COUNT(*) as job_count
                FROM jobs
                WHERE posted_date IS NOT NULL
                GROUP BY period
                ORDER BY period
            """),
            {"trunc": trunc},
        ).fetchall()
        return [{"period": str(r[0]), "job_count": r[1]} for r in rows]


def get_new_jobs_since(hours: int = 24) -> list:
    """Return jobs first seen within the last N hours."""
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT id, title, company, location, posted_date, first_seen_at
                FROM jobs
                WHERE first_seen_at >= NOW() - INTERVAL ':hours hours'
                ORDER BY first_seen_at DESC
            """),
            {"hours": hours},
        ).fetchall()
        return [dict(r._mapping) for r in rows]


def get_contract_type_breakdown() -> list:
    """Return job counts grouped by contract type."""
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT
                    COALESCE(contract_type, 'Not specified') as contract_type,
                    COUNT(*) as job_count
                FROM jobs
                GROUP BY contract_type
                ORDER BY job_count DESC
            """)
        ).fetchall()
        return [{"contract_type": r[0], "job_count": r[1]} for r in rows]


def get_pipeline_history(limit: int = 20) -> list:
    """Return recent scrape run history."""
    with get_session() as session:
        rows = session.execute(
            text("""
                SELECT id, run_date, jobs_fetched, new_jobs, duplicates, status, error_message
                FROM scrape_runs
                ORDER BY run_date DESC
                LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()
        return [dict(r._mapping) for r in rows]

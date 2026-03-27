# Database utilities and connection management
import hashlib
import logging
from datetime import datetime
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.config.settings import DATABASE_URL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine & session factory (connection pool built-in via SQLAlchemy)
# ---------------------------------------------------------------------------
engine = create_engine(DATABASE_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)
SessionFactory = sessionmaker(bind=engine)


@contextmanager
def get_session():
    """Provide a transactional database session that auto-closes."""
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Raw layer – land API responses as-is
# ---------------------------------------------------------------------------
def insert_raw_jobs(job_list: list, source: str = "adzuna") -> int:
    """Insert raw job records into the staging table. Returns count inserted."""
    with get_session() as session:
        for job in job_list:
            session.execute(
                text("""
                    INSERT INTO raw_jobs (job_data, source)
                    VALUES (:job_data, :source)
                """),
                {"job_data": str(job), "source": source},  # JSONB accepts string
            )
        count = len(job_list)
        logger.info(f"Inserted {count} raw jobs")
        return count


# ---------------------------------------------------------------------------
# Cleaned layer – deduplicated inserts
# ---------------------------------------------------------------------------
def _hash_url(url: str) -> str:
    """Create a SHA-256 hash of the job URL for deduplication."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def upsert_jobs(job_list: list) -> dict:
    """
    Insert or update cleaned jobs. Deduplicates by url_hash.
    Returns {"new": n, "updated": n, "duplicates": n}.
    """
    stats = {"new": 0, "updated": 0, "duplicates": 0}

    with get_session() as session:
        for job in job_list:
            url = job.get("redirect_url", "")
            url_hash = _hash_url(url)

            existing = session.execute(
                text("SELECT id, last_seen_at FROM jobs WHERE url_hash = :h"),
                {"h": url_hash},
            ).fetchone()

            if existing:
                # Job already exists – update last_seen_at timestamp
                session.execute(
                    text("UPDATE jobs SET last_seen_at = :now WHERE id = :id"),
                    {"now": datetime.utcnow(), "id": existing[0]},
                )
                stats["updated"] += 1
            else:
                # New job – insert
                salary_min = job.get("salary_min")
                salary_max = job.get("salary_max")
                category = job.get("category", {}).get("label", None) if isinstance(job.get("category"), dict) else None

                session.execute(
                    text("""
                        INSERT INTO jobs
                            (adzuna_id, url, url_hash, title, company, location,
                             description, salary_min, salary_max, contract_type,
                             category, posted_date)
                        VALUES
                            (:adzuna_id, :url, :url_hash, :title, :company, :location,
                             :description, :salary_min, :salary_max, :contract_type,
                             :category, :posted_date)
                    """),
                    {
                        "adzuna_id": str(job.get("id", "")),
                        "url": url,
                        "url_hash": url_hash,
                        "title": job.get("title", ""),
                        "company": job.get("company", {}).get("display_name", None) if isinstance(job.get("company"), dict) else None,
                        "location": job.get("location", {}).get("display_name", None) if isinstance(job.get("location"), dict) else None,
                        "description": job.get("description", ""),
                        "salary_min": float(salary_min) if salary_min else None,
                        "salary_max": float(salary_max) if salary_max else None,
                        "contract_type": job.get("contract_type"),
                        "category": category,
                        "posted_date": job.get("created"),
                    },
                )
                stats["new"] += 1

    stats["duplicates"] = stats["updated"]  # seen-before count
    logger.info(f"Upsert complete: {stats}")
    return stats


# ---------------------------------------------------------------------------
# Scrape run tracking
# ---------------------------------------------------------------------------
def log_scrape_run(jobs_fetched: int, new_jobs: int, duplicates: int,
                   status: str = "success", error_message: str = None):
    """Record metadata about a pipeline run."""
    with get_session() as session:
        session.execute(
            text("""
                INSERT INTO scrape_runs (jobs_fetched, new_jobs, duplicates, status, error_message)
                VALUES (:fetched, :new, :dupes, :status, :err)
            """),
            {
                "fetched": jobs_fetched,
                "new": new_jobs,
                "dupes": duplicates,
                "status": status,
                "err": error_message,
            },
        )
    logger.info(f"Scrape run logged: {status}")


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def get_job_count() -> int:
    """Return total number of cleaned jobs in the database."""
    with get_session() as session:
        result = session.execute(text("SELECT COUNT(*) FROM jobs")).scalar()
        return result


def get_recent_jobs(limit: int = 10) -> list:
    """Return the most recently posted jobs."""
    with get_session() as session:
        rows = session.execute(
            text("SELECT id, title, company, location, posted_date FROM jobs ORDER BY posted_date DESC LIMIT :lim"),
            {"lim": limit},
        ).fetchall()
        return [dict(row._mapping) for row in rows]

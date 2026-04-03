# Data cleaning and preprocessing utilities
import re
import logging
from datetime import datetime
from typing import Optional

from src.validation.schemas import (
    validate_raw_batch,
    validate_cleaned_batch,
    ValidationResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field-level cleaners
# ---------------------------------------------------------------------------
def clean_title(title: Optional[str]) -> Optional[str]:
    """Normalize job title: strip whitespace, collapse internal spaces."""
    if not title:
        return None
    title = re.sub(r"\s+", " ", title.strip())
    return title if title else None


def clean_company(company_field) -> Optional[str]:
    """Extract and normalize company name from Adzuna's nested dict or string."""
    if isinstance(company_field, dict):
        name = company_field.get("display_name", "")
    elif isinstance(company_field, str):
        name = company_field
    else:
        return None
    name = name.strip()
    return name if name else None


def clean_location(location_field) -> Optional[str]:
    """Extract and normalize location from Adzuna's nested dict or string."""
    if isinstance(location_field, dict):
        name = location_field.get("display_name", "")
    elif isinstance(location_field, str):
        name = location_field
    else:
        return None
    name = name.strip()
    return name if name else None


def clean_salary(value) -> Optional[float]:
    """Parse salary to float, return None if invalid or negative."""
    if value is None:
        return None
    try:
        salary = float(value)
        return salary if salary >= 0 else None
    except (ValueError, TypeError):
        return None


def clean_description(desc: Optional[str]) -> Optional[str]:
    """Strip HTML tags and normalize whitespace in description text."""
    if not desc:
        return None
    # Remove HTML tags
    cleaned = re.sub(r"<[^>]+>", " ", desc)
    # Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned if cleaned else None


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """
    Parse ISO date string into a normalized format.
    Returns ISO string or None if unparseable.
    """
    if not date_str:
        return None
    try:
        # Handle various ISO formats from Adzuna
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, AttributeError):
        return None


def extract_category(category_field) -> Optional[str]:
    """Extract category label from Adzuna's nested dict or string."""
    if isinstance(category_field, dict):
        return category_field.get("label")
    elif isinstance(category_field, str):
        return category_field
    return None


# ---------------------------------------------------------------------------
# Required fields validation
# ---------------------------------------------------------------------------
REQUIRED_FIELDS = ["title", "redirect_url"]


def validate_job(job: dict) -> bool:
    """Check that a job record has all required fields with non-empty values."""
    for field in REQUIRED_FIELDS:
        value = job.get(field)
        if not value or (isinstance(value, str) and not value.strip()):
            logger.debug(f"Job missing required field '{field}': {job.get('id', 'unknown')}")
            return False
    return True


# ---------------------------------------------------------------------------
# Full record cleaner
# ---------------------------------------------------------------------------
def clean_job(raw_job: dict) -> Optional[dict]:
    """
    Validate and clean a single raw job record.
    Returns a cleaned dict ready for database insert, or None if invalid.
    """
    if not validate_job(raw_job):
        return None

    return {
        "id": str(raw_job.get("id", "")),
        "redirect_url": raw_job.get("redirect_url", ""),
        "title": clean_title(raw_job.get("title")),
        "company": clean_company(raw_job.get("company")),
        "location": clean_location(raw_job.get("location")),
        "description": clean_description(raw_job.get("description")),
        "salary_min": clean_salary(raw_job.get("salary_min")),
        "salary_max": clean_salary(raw_job.get("salary_max")),
        "contract_type": raw_job.get("contract_type"),
        "category": extract_category(raw_job.get("category")),
        "created": parse_date(raw_job.get("created")),
    }


def clean_job_batch(raw_jobs: list) -> list:
    """
    Clean a batch of raw job records with two-stage Pydantic validation.

    Stage 1: Validate raw API records (reject malformed input)
    Stage 2: Clean valid records
    Stage 3: Validate cleaned output (catch transform bugs)

    Returns list of cleaned dicts that passed both validation gates.
    """
    # Stage 1: validate raw input
    raw_result = validate_raw_batch(raw_jobs)
    if raw_result.error_count:
        logger.warning(
            f"Raw validation rejected {raw_result.error_count}/{raw_result.total} records"
        )

    # Stage 2: clean the validated records
    cleaned = []
    skipped = 0
    for job in raw_result.valid:
        result = clean_job(job)
        if result:
            cleaned.append(result)
        else:
            skipped += 1

    # Stage 3: validate cleaned output before database insertion
    cleaned_result = validate_cleaned_batch(cleaned)
    if cleaned_result.error_count:
        logger.warning(
            f"Cleaned validation rejected {cleaned_result.error_count}/{cleaned_result.total} records"
        )

    logger.info(
        f"Pipeline: {len(raw_jobs)} raw → {raw_result.valid_count} passed raw validation → "
        f"{len(cleaned)} cleaned → {cleaned_result.valid_count} passed final validation "
        f"({len(raw_jobs) - cleaned_result.valid_count} total rejected)"
    )
    return cleaned_result.valid

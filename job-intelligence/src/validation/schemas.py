# Pydantic schemas for data quality enforcement
# Validates records at two stages:
#   1. RawJobRecord   – what comes back from the Adzuna API
#   2. CleanedJobRecord – what we insert into the 'jobs' table
#
# Records that fail validation are rejected with clear error messages
# rather than silently corrupting downstream tables.

import re
import logging
from typing import Optional, Union
from datetime import datetime

from pydantic import BaseModel, field_validator, model_validator, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage 1: Raw API response validation
# ---------------------------------------------------------------------------
class AdzunaCompany(BaseModel):
    """Nested company object from Adzuna API."""
    display_name: Optional[str] = None


class AdzunaLocation(BaseModel):
    """Nested location object from Adzuna API."""
    display_name: Optional[str] = None
    area: Optional[list] = None


class AdzunaCategory(BaseModel):
    """Nested category object from Adzuna API."""
    label: Optional[str] = None
    tag: Optional[str] = None


class RawJobRecord(BaseModel):
    """
    Schema for a single job record from the Adzuna API.
    Enforces that required fields exist and have valid types.
    Rejects records missing title or redirect_url.
    """
    id: Union[str, int]
    redirect_url: str
    title: str
    description: Optional[str] = None
    company: Optional[Union[AdzunaCompany, str]] = None
    location: Optional[Union[AdzunaLocation, str]] = None
    salary_min: Optional[Union[float, int]] = None
    salary_max: Optional[Union[float, int]] = None
    contract_type: Optional[str] = None
    category: Optional[Union[AdzunaCategory, str]] = None
    created: Optional[str] = None

    model_config = {"extra": "allow"}  # API may return fields we don't use

    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("title cannot be empty")
        return v

    @field_validator("redirect_url")
    @classmethod
    def url_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("redirect_url cannot be empty")
        return v

    @field_validator("salary_min", "salary_max")
    @classmethod
    def salary_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("salary cannot be negative")
        return v

    @model_validator(mode="after")
    def salary_min_lte_max(self):
        if self.salary_min is not None and self.salary_max is not None:
            if self.salary_min > self.salary_max:
                logger.warning(
                    f"Job {self.id}: salary_min ({self.salary_min}) > salary_max ({self.salary_max}), swapping"
                )
                self.salary_min, self.salary_max = self.salary_max, self.salary_min
        return self


# ---------------------------------------------------------------------------
# Stage 2: Cleaned record validation (post-transform, pre-database)
# ---------------------------------------------------------------------------
class CleanedJobRecord(BaseModel):
    """
    Schema for a cleaned job record ready for database insertion.
    Enforces stricter rules than the raw schema — fields have been
    through clean_data.py and must be in their final form.
    """
    id: str
    redirect_url: str
    title: str
    company: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    contract_type: Optional[str] = None
    category: Optional[str] = None
    created: Optional[str] = None

    @field_validator("title")
    @classmethod
    def title_is_clean(cls, v):
        if not v or not v.strip():
            raise ValueError("cleaned title cannot be empty")
        # Should not contain HTML tags after cleaning
        if re.search(r"<[^>]+>", v):
            raise ValueError("cleaned title still contains HTML tags")
        return v

    @field_validator("redirect_url")
    @classmethod
    def url_format(cls, v):
        if not v or not v.strip():
            raise ValueError("redirect_url cannot be empty")
        if not v.startswith(("http://", "https://")):
            raise ValueError(f"redirect_url must start with http(s)://, got: {v[:50]}")
        return v

    @field_validator("salary_min", "salary_max")
    @classmethod
    def salary_reasonable(cls, v):
        if v is not None:
            if v < 0:
                raise ValueError("salary cannot be negative")
            if v > 10_000_000:
                raise ValueError(f"salary suspiciously high: {v}")
        return v

    @field_validator("description")
    @classmethod
    def description_no_html(cls, v):
        if v and re.search(r"<[^>]+>", v):
            raise ValueError("cleaned description still contains HTML tags")
        return v


# ---------------------------------------------------------------------------
# Batch validation helpers
# ---------------------------------------------------------------------------
class ValidationResult:
    """Tracks validation outcomes for a batch of records."""

    def __init__(self):
        self.valid = []
        self.errors = []

    @property
    def valid_count(self):
        return len(self.valid)

    @property
    def error_count(self):
        return len(self.errors)

    @property
    def total(self):
        return self.valid_count + self.error_count

    def summary(self) -> dict:
        return {
            "total": self.total,
            "valid": self.valid_count,
            "rejected": self.error_count,
            "rejection_rate": round(self.error_count / self.total * 100, 1) if self.total else 0,
            "sample_errors": self.errors[:5],  # first 5 errors for debugging
        }


def validate_raw_batch(raw_jobs: list) -> ValidationResult:
    """
    Validate a batch of raw API records against RawJobRecord schema.
    Returns a ValidationResult with valid records and error details.
    """
    result = ValidationResult()

    for i, job in enumerate(raw_jobs):
        try:
            validated = RawJobRecord(**job)
            result.valid.append(validated.model_dump())
        except Exception as e:
            job_id = job.get("id", f"index_{i}")
            error_msg = str(e)
            result.errors.append({"job_id": job_id, "error": error_msg})
            logger.warning(f"Raw validation failed for job {job_id}: {error_msg}")

    logger.info(
        f"Raw validation: {result.valid_count}/{result.total} passed, "
        f"{result.error_count} rejected"
    )
    return result


def validate_cleaned_batch(cleaned_jobs: list) -> ValidationResult:
    """
    Validate a batch of cleaned records against CleanedJobRecord schema.
    This is the final gate before database insertion.
    """
    result = ValidationResult()

    for i, job in enumerate(cleaned_jobs):
        try:
            validated = CleanedJobRecord(**job)
            result.valid.append(validated.model_dump())
        except Exception as e:
            job_id = job.get("id", f"index_{i}")
            error_msg = str(e)
            result.errors.append({"job_id": job_id, "error": error_msg})
            logger.warning(f"Cleaned validation failed for job {job_id}: {error_msg}")

    logger.info(
        f"Cleaned validation: {result.valid_count}/{result.total} passed, "
        f"{result.error_count} rejected"
    )
    return result

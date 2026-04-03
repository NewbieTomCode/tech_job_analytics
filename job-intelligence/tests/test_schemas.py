# Tests for Pydantic validation schemas
import pytest
from pydantic import ValidationError

from src.validation.schemas import (
    RawJobRecord,
    CleanedJobRecord,
    ValidationResult,
    validate_raw_batch,
    validate_cleaned_batch,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
VALID_RAW_JOB = {
    "id": "12345",
    "redirect_url": "https://adzuna.com/job/12345",
    "title": "Data Engineer",
    "description": "Build data pipelines using Python and SQL",
    "company": {"display_name": "Acme Corp"},
    "location": {"display_name": "London"},
    "salary_min": 50000,
    "salary_max": 70000,
    "contract_type": "permanent",
    "category": {"label": "IT Jobs", "tag": "it-jobs"},
    "created": "2025-03-15T00:00:00Z",
}

VALID_CLEANED_JOB = {
    "id": "12345",
    "redirect_url": "https://adzuna.com/job/12345",
    "title": "Data Engineer",
    "description": "Build data pipelines using Python and SQL",
    "company": "Acme Corp",
    "location": "London",
    "salary_min": 50000.0,
    "salary_max": 70000.0,
    "contract_type": "permanent",
    "category": "IT Jobs",
    "created": "2025-03-15T00:00:00+00:00",
}


# ---------------------------------------------------------------------------
# RawJobRecord — valid cases
# ---------------------------------------------------------------------------
class TestRawJobRecordValid:
    def test_full_record(self):
        record = RawJobRecord(**VALID_RAW_JOB)
        assert record.title == "Data Engineer"
        assert record.salary_min == 50000

    def test_minimal_record(self):
        record = RawJobRecord(id="1", redirect_url="https://x.com/1", title="Dev")
        assert record.title == "Dev"
        assert record.salary_min is None
        assert record.company is None

    def test_integer_id(self):
        record = RawJobRecord(id=99, redirect_url="https://x.com", title="Dev")
        assert record.id == 99

    def test_string_company(self):
        job = {**VALID_RAW_JOB, "company": "Plain String Co"}
        record = RawJobRecord(**job)
        assert record.company == "Plain String Co"

    def test_string_location(self):
        job = {**VALID_RAW_JOB, "location": "Manchester"}
        record = RawJobRecord(**job)
        assert record.location == "Manchester"

    def test_string_category(self):
        job = {**VALID_RAW_JOB, "category": "Engineering"}
        record = RawJobRecord(**job)
        assert record.category == "Engineering"

    def test_extra_fields_allowed(self):
        job = {**VALID_RAW_JOB, "some_unknown_field": "surprise"}
        record = RawJobRecord(**job)
        assert record.title == "Data Engineer"

    def test_salary_swap_when_min_gt_max(self):
        job = {**VALID_RAW_JOB, "salary_min": 80000, "salary_max": 50000}
        record = RawJobRecord(**job)
        assert record.salary_min == 50000
        assert record.salary_max == 80000


# ---------------------------------------------------------------------------
# RawJobRecord — rejection cases
# ---------------------------------------------------------------------------
class TestRawJobRecordRejection:
    def test_missing_title(self):
        with pytest.raises(ValidationError):
            RawJobRecord(id="1", redirect_url="https://x.com")

    def test_empty_title(self):
        with pytest.raises(ValidationError, match="title cannot be empty"):
            RawJobRecord(id="1", redirect_url="https://x.com", title="")

    def test_whitespace_title(self):
        with pytest.raises(ValidationError, match="title cannot be empty"):
            RawJobRecord(id="1", redirect_url="https://x.com", title="   ")

    def test_missing_redirect_url(self):
        with pytest.raises(ValidationError):
            RawJobRecord(id="1", title="Dev")

    def test_empty_redirect_url(self):
        with pytest.raises(ValidationError, match="redirect_url cannot be empty"):
            RawJobRecord(id="1", redirect_url="", title="Dev")

    def test_missing_id(self):
        with pytest.raises(ValidationError):
            RawJobRecord(redirect_url="https://x.com", title="Dev")

    def test_negative_salary_min(self):
        with pytest.raises(ValidationError, match="salary cannot be negative"):
            RawJobRecord(
                id="1", redirect_url="https://x.com", title="Dev",
                salary_min=-5000,
            )

    def test_negative_salary_max(self):
        with pytest.raises(ValidationError, match="salary cannot be negative"):
            RawJobRecord(
                id="1", redirect_url="https://x.com", title="Dev",
                salary_max=-1000,
            )


# ---------------------------------------------------------------------------
# CleanedJobRecord — valid cases
# ---------------------------------------------------------------------------
class TestCleanedJobRecordValid:
    def test_full_record(self):
        record = CleanedJobRecord(**VALID_CLEANED_JOB)
        assert record.title == "Data Engineer"
        assert record.salary_min == 50000.0

    def test_minimal_record(self):
        record = CleanedJobRecord(
            id="1", redirect_url="https://x.com/1", title="Dev"
        )
        assert record.company is None

    def test_none_salary(self):
        job = {**VALID_CLEANED_JOB, "salary_min": None, "salary_max": None}
        record = CleanedJobRecord(**job)
        assert record.salary_min is None


# ---------------------------------------------------------------------------
# CleanedJobRecord — rejection cases
# ---------------------------------------------------------------------------
class TestCleanedJobRecordRejection:
    def test_empty_title(self):
        with pytest.raises(ValidationError, match="cleaned title cannot be empty"):
            CleanedJobRecord(id="1", redirect_url="https://x.com", title="")

    def test_html_in_title(self):
        with pytest.raises(ValidationError, match="HTML tags"):
            CleanedJobRecord(
                id="1", redirect_url="https://x.com",
                title="<b>Data Engineer</b>",
            )

    def test_invalid_url_format(self):
        with pytest.raises(ValidationError, match="http"):
            CleanedJobRecord(id="1", redirect_url="not-a-url", title="Dev")

    def test_html_in_description(self):
        with pytest.raises(ValidationError, match="HTML tags"):
            CleanedJobRecord(
                id="1", redirect_url="https://x.com", title="Dev",
                description="<p>Some HTML</p>",
            )

    def test_suspiciously_high_salary(self):
        with pytest.raises(ValidationError, match="suspiciously high"):
            CleanedJobRecord(
                id="1", redirect_url="https://x.com", title="Dev",
                salary_min=50_000_000.0,
            )

    def test_negative_salary(self):
        with pytest.raises(ValidationError, match="salary cannot be negative"):
            CleanedJobRecord(
                id="1", redirect_url="https://x.com", title="Dev",
                salary_min=-1000.0,
            )


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------
class TestValidationResult:
    def test_empty_result(self):
        r = ValidationResult()
        assert r.valid_count == 0
        assert r.error_count == 0
        assert r.total == 0

    def test_summary(self):
        r = ValidationResult()
        r.valid.append({"id": "1"})
        r.valid.append({"id": "2"})
        r.errors.append({"job_id": "3", "error": "bad"})

        summary = r.summary()
        assert summary["total"] == 3
        assert summary["valid"] == 2
        assert summary["rejected"] == 1
        assert summary["rejection_rate"] == 33.3

    def test_summary_empty(self):
        r = ValidationResult()
        summary = r.summary()
        assert summary["rejection_rate"] == 0

    def test_sample_errors_capped_at_five(self):
        r = ValidationResult()
        for i in range(10):
            r.errors.append({"job_id": str(i), "error": "bad"})
        assert len(r.summary()["sample_errors"]) == 5


# ---------------------------------------------------------------------------
# validate_raw_batch
# ---------------------------------------------------------------------------
class TestValidateRawBatch:
    def test_all_valid(self):
        jobs = [VALID_RAW_JOB, {**VALID_RAW_JOB, "id": "99999"}]
        result = validate_raw_batch(jobs)
        assert result.valid_count == 2
        assert result.error_count == 0

    def test_mixed_batch(self):
        bad_job = {"id": "bad", "title": ""}  # missing redirect_url, empty title
        result = validate_raw_batch([VALID_RAW_JOB, bad_job])
        assert result.valid_count == 1
        assert result.error_count == 1

    def test_all_invalid(self):
        result = validate_raw_batch([{"garbage": True}, {"also": "bad"}])
        assert result.valid_count == 0
        assert result.error_count == 2

    def test_empty_batch(self):
        result = validate_raw_batch([])
        assert result.valid_count == 0
        assert result.error_count == 0

    def test_preserves_extra_fields(self):
        job = {**VALID_RAW_JOB, "latitude": 51.5}
        result = validate_raw_batch([job])
        assert result.valid_count == 1
        assert "latitude" in result.valid[0]


# ---------------------------------------------------------------------------
# validate_cleaned_batch
# ---------------------------------------------------------------------------
class TestValidateCleanedBatch:
    def test_all_valid(self):
        result = validate_cleaned_batch([VALID_CLEANED_JOB])
        assert result.valid_count == 1
        assert result.error_count == 0

    def test_rejects_html_in_cleaned(self):
        bad = {**VALID_CLEANED_JOB, "title": "<b>Dirty</b>"}
        result = validate_cleaned_batch([bad])
        assert result.valid_count == 0
        assert result.error_count == 1

    def test_rejects_bad_url(self):
        bad = {**VALID_CLEANED_JOB, "redirect_url": "ftp://weird"}
        result = validate_cleaned_batch([bad])
        assert result.valid_count == 0
        assert result.error_count == 1

    def test_empty_batch(self):
        result = validate_cleaned_batch([])
        assert result.valid_count == 0
        assert result.error_count == 0

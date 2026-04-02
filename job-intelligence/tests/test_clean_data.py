# Tests for data cleaning and preprocessing
import pytest
from src.transform.clean_data import (
    clean_title,
    clean_company,
    clean_location,
    clean_salary,
    clean_description,
    parse_date,
    extract_category,
    validate_job,
    clean_job,
    clean_job_batch,
)


# ---------------------------------------------------------------------------
# clean_title
# ---------------------------------------------------------------------------
class TestCleanTitle:
    def test_strips_whitespace(self):
        assert clean_title("  Data Engineer  ") == "Data Engineer"

    def test_collapses_internal_spaces(self):
        assert clean_title("Data    Engineer   Role") == "Data Engineer Role"

    def test_none_returns_none(self):
        assert clean_title(None) is None

    def test_empty_string_returns_none(self):
        assert clean_title("") is None

    def test_whitespace_only_returns_none(self):
        assert clean_title("   ") is None

    def test_normal_title_unchanged(self):
        assert clean_title("Senior Data Engineer") == "Senior Data Engineer"

    def test_tabs_and_newlines(self):
        assert clean_title("Data\t\tEngineer\n") == "Data Engineer"

    def test_unicode_characters(self):
        assert clean_title("Développeur Python") == "Développeur Python"


# ---------------------------------------------------------------------------
# clean_company
# ---------------------------------------------------------------------------
class TestCleanCompany:
    def test_dict_with_display_name(self):
        assert clean_company({"display_name": "Acme Corp"}) == "Acme Corp"

    def test_string_input(self):
        assert clean_company("Acme Corp") == "Acme Corp"

    def test_dict_empty_name(self):
        assert clean_company({"display_name": ""}) is None

    def test_none_input(self):
        assert clean_company(None) is None

    def test_numeric_input(self):
        assert clean_company(123) is None

    def test_strips_whitespace(self):
        assert clean_company("  Acme Corp  ") == "Acme Corp"

    def test_dict_missing_display_name(self):
        assert clean_company({"other_key": "value"}) is None


# ---------------------------------------------------------------------------
# clean_location
# ---------------------------------------------------------------------------
class TestCleanLocation:
    def test_dict_with_display_name(self):
        assert clean_location({"display_name": "London"}) == "London"

    def test_string_input(self):
        assert clean_location("Manchester") == "Manchester"

    def test_none_input(self):
        assert clean_location(None) is None

    def test_empty_dict_name(self):
        assert clean_location({"display_name": ""}) is None

    def test_list_input(self):
        assert clean_location([1, 2, 3]) is None

    def test_whitespace_only_string(self):
        assert clean_location("   ") is None


# ---------------------------------------------------------------------------
# clean_salary
# ---------------------------------------------------------------------------
class TestCleanSalary:
    def test_valid_int(self):
        assert clean_salary(50000) == 50000.0

    def test_valid_float(self):
        assert clean_salary(45000.50) == 45000.50

    def test_string_number(self):
        assert clean_salary("60000") == 60000.0

    def test_none_returns_none(self):
        assert clean_salary(None) is None

    def test_negative_returns_none(self):
        assert clean_salary(-5000) is None

    def test_zero_is_valid(self):
        assert clean_salary(0) == 0.0

    def test_invalid_string_returns_none(self):
        assert clean_salary("not_a_number") is None

    def test_empty_string_returns_none(self):
        assert clean_salary("") is None

    def test_very_large_salary(self):
        assert clean_salary(999999999) == 999999999.0

    def test_boolean_true(self):
        # bool is subclass of int in Python; True == 1
        result = clean_salary(True)
        assert result == 1.0

    def test_list_returns_none(self):
        assert clean_salary([50000]) is None


# ---------------------------------------------------------------------------
# clean_description
# ---------------------------------------------------------------------------
class TestCleanDescription:
    def test_strips_html_tags(self):
        result = clean_description("<p>Build <strong>data</strong> pipelines</p>")
        assert result == "Build data pipelines"

    def test_collapses_whitespace(self):
        result = clean_description("Build   data    pipelines")
        assert result == "Build data pipelines"

    def test_none_returns_none(self):
        assert clean_description(None) is None

    def test_empty_returns_none(self):
        assert clean_description("") is None

    def test_html_only_returns_none(self):
        # After stripping tags, only whitespace remains
        assert clean_description("<br><br>") is None

    def test_complex_html(self):
        html = "<div><h2>Requirements</h2><ul><li>Python</li><li>SQL</li></ul></div>"
        result = clean_description(html)
        assert "Requirements" in result
        assert "Python" in result
        assert "SQL" in result
        assert "<" not in result


# ---------------------------------------------------------------------------
# parse_date
# ---------------------------------------------------------------------------
class TestParseDate:
    def test_iso_format_with_z(self):
        result = parse_date("2025-03-15T10:00:00Z")
        assert result is not None
        assert "2025-03-15" in result

    def test_iso_format_with_offset(self):
        result = parse_date("2025-03-15T10:00:00+00:00")
        assert result is not None

    def test_none_returns_none(self):
        assert parse_date(None) is None

    def test_empty_string_returns_none(self):
        assert parse_date("") is None

    def test_invalid_date_returns_none(self):
        assert parse_date("not-a-date") is None

    def test_date_only_no_time(self):
        result = parse_date("2025-03-15")
        assert result is not None
        assert "2025-03-15" in result

    def test_numeric_input_returns_none(self):
        assert parse_date(12345) is None


# ---------------------------------------------------------------------------
# extract_category
# ---------------------------------------------------------------------------
class TestExtractCategory:
    def test_dict_with_label(self):
        assert extract_category({"label": "IT Jobs"}) == "IT Jobs"

    def test_string_input(self):
        assert extract_category("Engineering") == "Engineering"

    def test_none_input(self):
        assert extract_category(None) is None

    def test_dict_no_label(self):
        assert extract_category({"tag": "tech"}) is None


# ---------------------------------------------------------------------------
# validate_job
# ---------------------------------------------------------------------------
class TestValidateJob:
    def test_valid_job(self):
        job = {"title": "Data Engineer", "redirect_url": "https://example.com/job/1"}
        assert validate_job(job) is True

    def test_missing_title(self):
        job = {"redirect_url": "https://example.com/job/1"}
        assert validate_job(job) is False

    def test_missing_url(self):
        job = {"title": "Data Engineer"}
        assert validate_job(job) is False

    def test_empty_title(self):
        job = {"title": "", "redirect_url": "https://example.com/job/1"}
        assert validate_job(job) is False

    def test_whitespace_title(self):
        job = {"title": "   ", "redirect_url": "https://example.com/job/1"}
        assert validate_job(job) is False

    def test_none_url(self):
        job = {"title": "Data Engineer", "redirect_url": None}
        assert validate_job(job) is False


# ---------------------------------------------------------------------------
# clean_job (full record)
# ---------------------------------------------------------------------------
SAMPLE_RAW_JOB = {
    "id": "12345",
    "redirect_url": "https://adzuna.com/job/12345",
    "title": "  Data Engineer  ",
    "company": {"display_name": "Acme Corp"},
    "location": {"display_name": "London"},
    "description": "<p>Build <strong>data</strong> pipelines</p>",
    "salary_min": 40000,
    "salary_max": 60000,
    "contract_type": "permanent",
    "category": {"label": "IT Jobs"},
    "created": "2025-03-15T10:00:00Z",
}


class TestCleanJob:
    def test_cleans_all_fields(self):
        result = clean_job(SAMPLE_RAW_JOB)
        assert result is not None
        assert result["title"] == "Data Engineer"
        assert result["company"] == "Acme Corp"
        assert result["location"] == "London"
        assert result["description"] == "Build data pipelines"
        assert result["salary_min"] == 40000.0
        assert result["salary_max"] == 60000.0
        assert result["category"] == "IT Jobs"
        assert "2025-03-15" in result["created"]

    def test_returns_none_for_invalid(self):
        bad_job = {"id": "99", "description": "something"}
        assert clean_job(bad_job) is None

    def test_handles_missing_optional_fields(self):
        minimal = {
            "id": "1",
            "title": "Intern",
            "redirect_url": "https://example.com/1",
        }
        result = clean_job(minimal)
        assert result is not None
        assert result["title"] == "Intern"
        assert result["company"] is None
        assert result["salary_min"] is None


# ---------------------------------------------------------------------------
# clean_job_batch
# ---------------------------------------------------------------------------
class TestCleanJobBatch:
    def test_filters_invalid_records(self):
        jobs = [
            SAMPLE_RAW_JOB,
            {"id": "bad", "description": "no title or url"},
            {**SAMPLE_RAW_JOB, "id": "2", "title": "Dev"},
        ]
        result = clean_job_batch(jobs)
        assert len(result) == 2

    def test_empty_input(self):
        assert clean_job_batch([]) == []

    def test_all_invalid(self):
        jobs = [{"id": "1"}, {"id": "2"}]
        assert clean_job_batch(jobs) == []

    def test_preserves_order(self):
        jobs = [
            {**SAMPLE_RAW_JOB, "id": "a", "title": "First"},
            {**SAMPLE_RAW_JOB, "id": "b", "title": "Second"},
        ]
        result = clean_job_batch(jobs)
        assert result[0]["title"] == "First"
        assert result[1]["title"] == "Second"

    def test_large_batch(self):
        """Verify batch processing handles many records without error."""
        jobs = [{**SAMPLE_RAW_JOB, "id": str(i)} for i in range(500)]
        result = clean_job_batch(jobs)
        assert len(result) == 500

    def test_mixed_valid_invalid_interleaved(self):
        jobs = [
            SAMPLE_RAW_JOB,
            {"id": "bad1"},
            {**SAMPLE_RAW_JOB, "id": "2"},
            {"id": "bad2"},
            {**SAMPLE_RAW_JOB, "id": "3"},
        ]
        result = clean_job_batch(jobs)
        assert len(result) == 3

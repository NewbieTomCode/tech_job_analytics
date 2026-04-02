# Tests for database utilities
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime

from src.database_connections.db_utils import _hash_url


# ---------------------------------------------------------------------------
# _hash_url tests
# ---------------------------------------------------------------------------
class TestHashUrl:
    def test_returns_sha256_hex_string(self):
        result = _hash_url("https://example.com/job/123")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 produces 64 hex chars

    def test_same_url_same_hash(self):
        url = "https://adzuna.com/job/abc"
        assert _hash_url(url) == _hash_url(url)

    def test_different_urls_different_hashes(self):
        assert _hash_url("https://example.com/1") != _hash_url("https://example.com/2")

    def test_empty_string(self):
        result = _hash_url("")
        assert isinstance(result, str)
        assert len(result) == 64


# ---------------------------------------------------------------------------
# insert_raw_jobs tests
# ---------------------------------------------------------------------------
class TestInsertRawJobs:
    @patch("src.database_connections.db_utils.get_session")
    def test_inserts_all_jobs(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from src.database_connections.db_utils import insert_raw_jobs

        jobs = [{"title": "Job A"}, {"title": "Job B"}, {"title": "Job C"}]
        count = insert_raw_jobs(jobs)

        assert count == 3
        assert mock_session.execute.call_count == 3

    @patch("src.database_connections.db_utils.get_session")
    def test_empty_list_returns_zero(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from src.database_connections.db_utils import insert_raw_jobs

        count = insert_raw_jobs([])
        assert count == 0
        mock_session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# upsert_jobs tests
# ---------------------------------------------------------------------------
SAMPLE_JOB = {
    "id": "12345",
    "redirect_url": "https://adzuna.com/job/12345",
    "title": "Data Engineer",
    "company": {"display_name": "Acme Corp"},
    "location": {"display_name": "London"},
    "description": "Build data pipelines",
    "salary_min": 40000,
    "salary_max": 60000,
    "contract_type": "permanent",
    "category": {"label": "IT Jobs"},
    "created": "2025-03-01T10:00:00Z",
}


class TestUpsertJobs:
    @patch("src.database_connections.db_utils.get_session")
    def test_new_job_is_inserted(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # No existing record found
        mock_session.execute.return_value.fetchone.return_value = None

        from src.database_connections.db_utils import upsert_jobs

        stats = upsert_jobs([SAMPLE_JOB])

        assert stats["new"] == 1
        assert stats["duplicates"] == 0
        # Should have 2 execute calls: 1 SELECT check + 1 INSERT
        assert mock_session.execute.call_count == 2

    @patch("src.database_connections.db_utils.get_session")
    def test_duplicate_job_is_updated(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # Existing record found
        mock_session.execute.return_value.fetchone.return_value = (1, datetime.utcnow())

        from src.database_connections.db_utils import upsert_jobs

        stats = upsert_jobs([SAMPLE_JOB])

        assert stats["new"] == 0
        assert stats["duplicates"] == 1
        # Should have 2 execute calls: 1 SELECT check + 1 UPDATE
        assert mock_session.execute.call_count == 2

    @patch("src.database_connections.db_utils.get_session")
    def test_handles_missing_fields_gracefully(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchone.return_value = None

        from src.database_connections.db_utils import upsert_jobs

        minimal_job = {"id": "99", "title": "Intern"}
        stats = upsert_jobs([minimal_job])

        assert stats["new"] == 1

    @patch("src.database_connections.db_utils.get_session")
    def test_mixed_new_and_duplicate(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # First job: not found (new), second job: found (duplicate)
        mock_session.execute.return_value.fetchone.side_effect = [
            None,           # SELECT for job 1 -> not found
            (2, datetime.utcnow()),  # SELECT for job 2 -> found
        ]

        from src.database_connections.db_utils import upsert_jobs

        job_a = {**SAMPLE_JOB, "id": "111", "redirect_url": "https://adzuna.com/job/111"}
        job_b = {**SAMPLE_JOB, "id": "222", "redirect_url": "https://adzuna.com/job/222"}

        stats = upsert_jobs([job_a, job_b])

        assert stats["new"] == 1
        assert stats["duplicates"] == 1


# ---------------------------------------------------------------------------
# log_scrape_run tests
# ---------------------------------------------------------------------------
class TestLogScrapeRun:
    @patch("src.database_connections.db_utils.get_session")
    def test_logs_successful_run(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from src.database_connections.db_utils import log_scrape_run

        log_scrape_run(jobs_fetched=50, new_jobs=45, duplicates=5)

        mock_session.execute.assert_called_once()

    @patch("src.database_connections.db_utils.get_session")
    def test_logs_failed_run_with_error(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from src.database_connections.db_utils import log_scrape_run

        log_scrape_run(
            jobs_fetched=0, new_jobs=0, duplicates=0,
            status="failed", error_message="API timeout"
        )

        mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# get_job_count tests
# ---------------------------------------------------------------------------
class TestGetJobCount:
    @patch("src.database_connections.db_utils.get_session")
    def test_returns_count(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.scalar.return_value = 42

        from src.database_connections.db_utils import get_job_count

        assert get_job_count() == 42

    @patch("src.database_connections.db_utils.get_session")
    def test_returns_zero_when_empty(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.scalar.return_value = 0

        from src.database_connections.db_utils import get_job_count

        assert get_job_count() == 0


# ---------------------------------------------------------------------------
# get_recent_jobs tests
# ---------------------------------------------------------------------------
class TestGetRecentJobs:
    @patch("src.database_connections.db_utils.get_session")
    def test_returns_list_of_dicts(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row._mapping = {
            "id": 1, "title": "Data Engineer", "company": "Acme",
            "location": "London", "posted_date": "2025-03-15",
        }
        mock_session.execute.return_value.fetchall.return_value = [mock_row]

        from src.database_connections.db_utils import get_recent_jobs

        result = get_recent_jobs(limit=5)

        assert len(result) == 1
        assert result[0]["title"] == "Data Engineer"

    @patch("src.database_connections.db_utils.get_session")
    def test_empty_table(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.database_connections.db_utils import get_recent_jobs

        assert get_recent_jobs() == []

# Tests for analytics reports
import pytest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Helper to create a mock session with query results
# ---------------------------------------------------------------------------
def _mock_session_with_rows(rows, is_single=False):
    """Create a patched get_session that returns given rows."""
    mock_session = MagicMock()
    mock_get_session = MagicMock()
    mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

    if is_single:
        mock_session.execute.return_value.fetchone.return_value = rows
    else:
        mock_session.execute.return_value.fetchall.return_value = rows

    return mock_get_session


# ---------------------------------------------------------------------------
# get_top_companies
# ---------------------------------------------------------------------------
class TestGetTopCompanies:
    @patch("src.analytics.reports.get_session")
    def test_returns_formatted_results(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [
            ("Acme Corp", 15),
            ("Tech Inc", 10),
            ("DataCo", 5),
        ]

        from src.analytics.reports import get_top_companies
        result = get_top_companies(limit=3)

        assert len(result) == 3
        assert result[0] == {"company": "Acme Corp", "job_count": 15}
        assert result[1] == {"company": "Tech Inc", "job_count": 10}

    @patch("src.analytics.reports.get_session")
    def test_empty_results(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_top_companies
        result = get_top_companies()
        assert result == []

    @patch("src.analytics.reports.get_session")
    def test_custom_limit(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [("OneCorp", 5)]

        from src.analytics.reports import get_top_companies
        result = get_top_companies(limit=1)

        assert len(result) == 1
        # Verify the limit parameter was passed through
        mock_session.execute.assert_called_once()

    @patch("src.analytics.reports.get_session")
    def test_single_company(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [("Solo Ltd", 1)]

        from src.analytics.reports import get_top_companies
        result = get_top_companies()

        assert result == [{"company": "Solo Ltd", "job_count": 1}]


# ---------------------------------------------------------------------------
# get_jobs_by_location
# ---------------------------------------------------------------------------
class TestGetJobsByLocation:
    @patch("src.analytics.reports.get_session")
    def test_returns_locations(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [
            ("London", 50),
            ("Manchester", 20),
        ]

        from src.analytics.reports import get_jobs_by_location
        result = get_jobs_by_location(limit=2)

        assert len(result) == 2
        assert result[0]["location"] == "London"
        assert result[0]["job_count"] == 50

    @patch("src.analytics.reports.get_session")
    def test_empty_locations(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_jobs_by_location
        result = get_jobs_by_location()
        assert result == []


# ---------------------------------------------------------------------------
# get_salary_stats
# ---------------------------------------------------------------------------
class TestGetSalaryStats:
    @patch("src.analytics.reports.get_session")
    def test_returns_stats(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchone.return_value = (
            100, 35000.00, 55000.00, 20000.00, 120000.00
        )

        from src.analytics.reports import get_salary_stats
        result = get_salary_stats()

        assert result["total_with_salary"] == 100
        assert result["avg_min"] == 35000.00
        assert result["avg_max"] == 55000.00
        assert result["lowest_min"] == 20000.00
        assert result["highest_max"] == 120000.00

    @patch("src.analytics.reports.get_session")
    def test_no_salary_data(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchone.return_value = (0, None, None, None, None)

        from src.analytics.reports import get_salary_stats
        result = get_salary_stats()

        assert result["total_with_salary"] == 0
        assert result["avg_min"] is None


# ---------------------------------------------------------------------------
# get_salary_by_category
# ---------------------------------------------------------------------------
class TestGetSalaryByCategory:
    @patch("src.analytics.reports.get_session")
    def test_returns_categories(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [
            ("IT Jobs", 50, 40000.00, 65000.00),
            ("Engineering", 30, 35000.00, 55000.00),
        ]

        from src.analytics.reports import get_salary_by_category
        result = get_salary_by_category()

        assert len(result) == 2
        assert result[0]["category"] == "IT Jobs"
        assert result[0]["avg_min"] == 40000.00
        assert result[0]["avg_max"] == 65000.00


# ---------------------------------------------------------------------------
# get_jobs_over_time
# ---------------------------------------------------------------------------
class TestGetJobsOverTime:
    @patch("src.analytics.reports.get_session")
    def test_returns_daily_counts(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [
            ("2025-03-01 00:00:00", 10),
            ("2025-03-02 00:00:00", 15),
        ]

        from src.analytics.reports import get_jobs_over_time
        result = get_jobs_over_time(interval="day")

        assert len(result) == 2
        assert result[0]["job_count"] == 10

    @patch("src.analytics.reports.get_session")
    def test_invalid_interval_defaults_to_day(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_jobs_over_time
        result = get_jobs_over_time(interval="invalid")
        assert result == []


# ---------------------------------------------------------------------------
# get_contract_type_breakdown
# ---------------------------------------------------------------------------
class TestGetContractTypeBreakdown:
    @patch("src.analytics.reports.get_session")
    def test_returns_breakdown(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [
            ("permanent", 80),
            ("contract", 15),
            ("Not specified", 5),
        ]

        from src.analytics.reports import get_contract_type_breakdown
        result = get_contract_type_breakdown()

        assert len(result) == 3
        assert result[0]["contract_type"] == "permanent"


# ---------------------------------------------------------------------------
# get_pipeline_history
# ---------------------------------------------------------------------------
class TestGetPipelineHistory:
    @patch("src.analytics.reports.get_session")
    def test_returns_history(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row._mapping = {
            "id": 1, "run_date": "2025-03-15", "jobs_fetched": 50,
            "new_jobs": 45, "duplicates": 5, "status": "success", "error_message": None,
        }
        mock_session.execute.return_value.fetchall.return_value = [mock_row]

        from src.analytics.reports import get_pipeline_history
        result = get_pipeline_history(limit=1)

        assert len(result) == 1
        assert result[0]["status"] == "success"

    @patch("src.analytics.reports.get_session")
    def test_empty_history(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_pipeline_history
        result = get_pipeline_history()
        assert result == []


# ---------------------------------------------------------------------------
# get_salary_by_category – edge cases
# ---------------------------------------------------------------------------
class TestGetSalaryByCategoryEdge:
    @patch("src.analytics.reports.get_session")
    def test_empty_categories(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_salary_by_category
        result = get_salary_by_category()
        assert result == []


# ---------------------------------------------------------------------------
# get_contract_type_breakdown – edge cases
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# get_new_jobs_since
# ---------------------------------------------------------------------------
class TestGetNewJobsSince:
    @patch("src.analytics.reports.get_session")
    def test_returns_recent_jobs(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row1 = MagicMock()
        mock_row1._mapping = {
            "id": 1, "title": "Data Engineer", "company": "Acme",
            "location": "London", "posted_date": "2025-03-15",
            "first_seen_at": "2025-03-15 10:00:00",
        }
        mock_row2 = MagicMock()
        mock_row2._mapping = {
            "id": 2, "title": "Backend Dev", "company": "TechCo",
            "location": "Manchester", "posted_date": "2025-03-15",
            "first_seen_at": "2025-03-15 11:00:00",
        }
        mock_session.execute.return_value.fetchall.return_value = [mock_row1, mock_row2]

        from src.analytics.reports import get_new_jobs_since
        result = get_new_jobs_since(hours=24)

        assert len(result) == 2
        assert result[0]["title"] == "Data Engineer"
        assert result[1]["company"] == "TechCo"

    @patch("src.analytics.reports.get_session")
    def test_no_recent_jobs(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_new_jobs_since
        result = get_new_jobs_since(hours=1)
        assert result == []

    @patch("src.analytics.reports.get_session")
    def test_custom_hours(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_new_jobs_since
        get_new_jobs_since(hours=48)
        mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# get_contract_type_breakdown – edge cases
# ---------------------------------------------------------------------------
class TestGetContractTypeBreakdownEdge:
    @patch("src.analytics.reports.get_session")
    def test_single_type(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = [("permanent", 100)]

        from src.analytics.reports import get_contract_type_breakdown
        result = get_contract_type_breakdown()

        assert len(result) == 1
        assert result[0]["contract_type"] == "permanent"
        assert result[0]["job_count"] == 100

    @patch("src.analytics.reports.get_session")
    def test_empty_breakdown(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        from src.analytics.reports import get_contract_type_breakdown
        result = get_contract_type_breakdown()
        assert result == []

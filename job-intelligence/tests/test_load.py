# Tests for the load step
import json
import pytest
from unittest.mock import patch, MagicMock


SAMPLE_JOBS = [
    {
        "id": "101",
        "redirect_url": "https://adzuna.com/job/101",
        "title": "Data Engineer",
        "company": {"display_name": "Acme Corp"},
        "location": {"display_name": "London"},
        "description": "Build pipelines",
        "salary_min": 45000,
        "salary_max": 65000,
        "category": {"label": "IT Jobs"},
        "created": "2025-03-15T09:00:00Z",
    },
    {
        "id": "102",
        "redirect_url": "https://adzuna.com/job/102",
        "title": "Software Developer",
        "company": {"display_name": "Tech Inc"},
        "location": {"display_name": "Manchester"},
        "description": "Write code",
        "salary_min": 35000,
        "salary_max": 55000,
        "category": {"label": "IT Jobs"},
        "created": "2025-03-16T10:00:00Z",
    },
]


# ---------------------------------------------------------------------------
# load_jobs tests
# ---------------------------------------------------------------------------
class TestLoadJobs:
    @patch("src.load.load_data.log_scrape_run")
    @patch("src.load.load_data.upsert_jobs")
    @patch("src.load.load_data.insert_raw_jobs")
    def test_full_pipeline(self, mock_insert_raw, mock_upsert, mock_log):
        mock_insert_raw.return_value = 2
        mock_upsert.return_value = {"new": 2, "updated": 0, "duplicates": 0}

        from src.load.load_data import load_jobs

        result = load_jobs(SAMPLE_JOBS)

        assert result["fetched"] == 2
        assert result["new"] == 2
        assert result["duplicates"] == 0
        mock_insert_raw.assert_called_once_with(SAMPLE_JOBS)
        mock_upsert.assert_called_once_with(SAMPLE_JOBS)
        mock_log.assert_called_once()

    @patch("src.load.load_data.log_scrape_run")
    @patch("src.load.load_data.upsert_jobs")
    @patch("src.load.load_data.insert_raw_jobs")
    def test_empty_list_skips_pipeline(self, mock_insert_raw, mock_upsert, mock_log):
        from src.load.load_data import load_jobs

        result = load_jobs([])

        assert result["fetched"] == 0
        assert result["new"] == 0
        mock_insert_raw.assert_not_called()
        mock_upsert.assert_not_called()
        mock_log.assert_not_called()

    @patch("src.load.load_data.log_scrape_run")
    @patch("src.load.load_data.upsert_jobs")
    @patch("src.load.load_data.insert_raw_jobs")
    def test_with_duplicates(self, mock_insert_raw, mock_upsert, mock_log):
        mock_insert_raw.return_value = 2
        mock_upsert.return_value = {"new": 1, "updated": 1, "duplicates": 1}

        from src.load.load_data import load_jobs

        result = load_jobs(SAMPLE_JOBS)

        assert result["new"] == 1
        assert result["duplicates"] == 1


# ---------------------------------------------------------------------------
# load_from_json tests
# ---------------------------------------------------------------------------
class TestLoadFromJson:
    @patch("src.load.load_data.load_jobs")
    def test_reads_file_and_calls_load(self, mock_load_jobs, tmp_path):
        mock_load_jobs.return_value = {"fetched": 2, "new": 2, "duplicates": 0}

        # Write a temp JSON file
        json_file = tmp_path / "test_jobs.json"
        json_file.write_text(json.dumps(SAMPLE_JOBS))

        from src.load.load_data import load_from_json

        result = load_from_json(str(json_file))

        assert result["fetched"] == 2
        mock_load_jobs.assert_called_once()
        # Verify the loaded data matches
        loaded_data = mock_load_jobs.call_args[0][0]
        assert len(loaded_data) == 2
        assert loaded_data[0]["title"] == "Data Engineer"

    @patch("src.load.load_data.load_jobs")
    def test_nonexistent_file_raises_error(self, mock_load_jobs):
        from src.load.load_data import load_from_json

        with pytest.raises(FileNotFoundError):
            load_from_json("/nonexistent/path.json")

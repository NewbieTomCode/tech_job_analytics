# Tests for fetching job data from API
import json
import pytest
from unittest.mock import patch, mock_open, MagicMock


# ---------------------------------------------------------------------------
# _check_credentials
# ---------------------------------------------------------------------------
class TestCheckCredentials:
    @patch("src.extract.fetch_data.ADZUNA_APP_KEY", "")
    @patch("src.extract.fetch_data.ADZUNA_APP_ID", "fake_id")
    def test_missing_key_raises(self):
        from src.extract.fetch_data import _check_credentials
        with pytest.raises(ValueError, match="Missing API credentials"):
            _check_credentials()

    @patch("src.extract.fetch_data.ADZUNA_APP_ID", "")
    @patch("src.extract.fetch_data.ADZUNA_APP_KEY", "fake_key")
    def test_missing_id_raises(self):
        from src.extract.fetch_data import _check_credentials
        with pytest.raises(ValueError, match="Missing API credentials"):
            _check_credentials()

    @patch("src.extract.fetch_data.ADZUNA_APP_ID", "")
    @patch("src.extract.fetch_data.ADZUNA_APP_KEY", "")
    def test_both_missing_raises(self):
        from src.extract.fetch_data import _check_credentials
        with pytest.raises(ValueError, match="Missing API credentials"):
            _check_credentials()

    @patch("src.extract.fetch_data.ADZUNA_APP_ID", "valid_id")
    @patch("src.extract.fetch_data.ADZUNA_APP_KEY", "valid_key")
    def test_valid_credentials_no_error(self):
        from src.extract.fetch_data import _check_credentials
        _check_credentials()  # should not raise


# ---------------------------------------------------------------------------
# store_job_data
# ---------------------------------------------------------------------------
class TestStoreJobData:
    @patch("builtins.open", new_callable=mock_open)
    def test_writes_json_to_file(self, mocked_file):
        from src.extract.fetch_data import store_job_data
        jobs = [{"title": "Engineer"}, {"title": "Analyst"}]
        store_job_data(jobs, filename="test_output.json")

        mocked_file.assert_called_once_with("test_output.json", "w")
        handle = mocked_file()
        written = "".join(call.args[0] for call in handle.write.call_args_list)
        parsed = json.loads(written)
        assert len(parsed) == 2
        assert parsed[0]["title"] == "Engineer"

    @patch("builtins.open", new_callable=mock_open)
    def test_empty_list(self, mocked_file):
        from src.extract.fetch_data import store_job_data
        store_job_data([], filename="empty.json")

        mocked_file.assert_called_once_with("empty.json", "w")
        handle = mocked_file()
        written = "".join(call.args[0] for call in handle.write.call_args_list)
        parsed = json.loads(written)
        assert parsed == []

    @patch("builtins.open", new_callable=mock_open)
    def test_default_filename(self, mocked_file):
        from src.extract.fetch_data import store_job_data
        store_job_data([{"id": 1}])

        mocked_file.assert_called_once_with("saved_data/input/all_jobs.json", "w")


# ---------------------------------------------------------------------------
# fetch_job_data – basic empty response
# ---------------------------------------------------------------------------
class TestFetchJobData:
    def test_empty_results(self, mocker):
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_ID", "fake_id")
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_KEY", "fake_key")

        mock_response = mocker.Mock()
        mock_response.json.return_value = {"count": 0, "results": []}
        mocker.patch("src.extract.fetch_data.requests.get", return_value=mock_response)
        mocker.patch("src.extract.fetch_data.store_job_data")

        from src.extract.fetch_data import fetch_job_data
        result = fetch_job_data("fake_url")
        assert result == []

    def test_single_page(self, mocker):
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_ID", "fake_id")
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_KEY", "fake_key")

        jobs = [{"id": 1, "title": "Dev"}, {"id": 2, "title": "Analyst"}]
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"count": 2, "results": jobs}
        mocker.patch("src.extract.fetch_data.requests.get", return_value=mock_response)
        mocker.patch("src.extract.fetch_data.store_job_data")

        from src.extract.fetch_data import fetch_job_data
        result = fetch_job_data()
        assert len(result) == 2
        assert result[0]["title"] == "Dev"

    def test_multi_page_pagination(self, mocker):
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_ID", "fake_id")
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_KEY", "fake_key")

        page1_jobs = [{"id": i, "title": f"Job {i}"} for i in range(50)]
        page2_jobs = [{"id": i, "title": f"Job {i}"} for i in range(50, 75)]

        # First call: initial count check returns 75 total
        # Second call: page 1 results
        # Third call: page 2 results
        resp_initial = mocker.Mock()
        resp_initial.json.return_value = {"count": 75, "results": page1_jobs}

        resp_page1 = mocker.Mock()
        resp_page1.json.return_value = {"count": 75, "results": page1_jobs}

        resp_page2 = mocker.Mock()
        resp_page2.json.return_value = {"count": 75, "results": page2_jobs}

        mocker.patch(
            "src.extract.fetch_data.requests.get",
            side_effect=[resp_initial, resp_page1, resp_page2],
        )
        mocker.patch("src.extract.fetch_data.store_job_data")

        from src.extract.fetch_data import fetch_job_data
        result = fetch_job_data()
        assert len(result) == 75  # initial call for count, then loop fetches page1(50) + page2(25)

    def test_calls_store_job_data(self, mocker):
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_ID", "fake_id")
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_KEY", "fake_key")

        mock_response = mocker.Mock()
        mock_response.json.return_value = {"count": 1, "results": [{"id": 1}]}
        mocker.patch("src.extract.fetch_data.requests.get", return_value=mock_response)
        mock_store = mocker.patch("src.extract.fetch_data.store_job_data")

        from src.extract.fetch_data import fetch_job_data
        fetch_job_data()
        mock_store.assert_called_once()

    def test_missing_credentials_raises(self, mocker):
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_ID", "")
        mocker.patch("src.extract.fetch_data.ADZUNA_APP_KEY", "")

        from src.extract.fetch_data import fetch_job_data
        with pytest.raises(ValueError, match="Missing API credentials"):
            fetch_job_data()

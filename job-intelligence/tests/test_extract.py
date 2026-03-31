# test fetching job data from API
import pytest
from src.extract.fetch_data import fetch_job_data

def test_fetch_job_data(mocker):
    # Mock credentials so the check passes
    mocker.patch("src.extract.fetch_data.ADZUNA_APP_ID", "fake_id")
    mocker.patch("src.extract.fetch_data.ADZUNA_APP_KEY", "fake_key")

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "count": 0,
        "results": []
    }

    # replace request.get to return fake data we defined above.
    # replace the request.get within the real function we import, any following tests that use request.get
    mocker.patch("src.extract.fetch_data.requests.get", return_value=mock_response)

    # mock store_job_data so it doesn't try to write to disk
    mocker.patch("src.extract.fetch_data.store_job_data")

    # call the function we want to test
    result = fetch_job_data("fake_url")

    # assert that the function returns the expected data
    assert result == []

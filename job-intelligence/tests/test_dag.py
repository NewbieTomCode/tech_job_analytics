# Tests for Airflow DAG task callables
import pytest
from unittest.mock import patch, MagicMock

import sys
import os
# Add dags/ to path so we can import the DAG module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "dags"))

# Skip entire module if airflow is not installed (CI / local dev without airflow)
airflow = pytest.importorskip("airflow", reason="Apache Airflow not installed")


SAMPLE_RAW_JOBS = [
    {
        "id": "101",
        "redirect_url": "https://adzuna.com/job/101",
        "title": "Data Engineer",
        "company": {"display_name": "Acme Corp"},
        "location": {"display_name": "London"},
        "description": "Build Python and SQL pipelines",
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
        "description": "Experience with Docker and Kubernetes",
        "salary_min": 35000,
        "salary_max": 55000,
        "category": {"label": "IT Jobs"},
        "created": "2025-03-16T10:00:00Z",
    },
]


# ---------------------------------------------------------------------------
# _fetch_jobs
# ---------------------------------------------------------------------------
class TestFetchJobsTask:
    @patch("src.extract.fetch_data.ADZUNA_APP_ID", "fake_id")
    @patch("src.extract.fetch_data.ADZUNA_APP_KEY", "fake_key")
    @patch("src.extract.fetch_data.store_job_data")
    @patch("src.extract.fetch_data.requests.get")
    def test_fetches_and_pushes_to_xcom(self, mock_get, mock_store):
        mock_response = MagicMock()
        mock_response.json.return_value = {"count": 0, "results": []}
        mock_get.return_value = mock_response

        from job_pipeline_dag import _fetch_jobs

        ti = MagicMock()
        result = _fetch_jobs(ti=ti)

        assert result == 0
        ti.xcom_push.assert_called_once_with(key="raw_jobs", value=[])


# ---------------------------------------------------------------------------
# _load_raw
# ---------------------------------------------------------------------------
class TestLoadRawTask:
    @patch("src.database_connections.db_utils.get_session")
    def test_inserts_raw_jobs(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from job_pipeline_dag import _load_raw

        ti = MagicMock()
        ti.xcom_pull.return_value = SAMPLE_RAW_JOBS

        result = _load_raw(ti=ti)

        assert result == 2
        ti.xcom_pull.assert_called_once_with(task_ids="fetch_jobs", key="raw_jobs")

    @patch("src.database_connections.db_utils.get_session")
    def test_empty_jobs_returns_zero(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from job_pipeline_dag import _load_raw

        ti = MagicMock()
        ti.xcom_pull.return_value = []

        result = _load_raw(ti=ti)
        assert result == 0


# ---------------------------------------------------------------------------
# _clean_and_upsert
# ---------------------------------------------------------------------------
class TestCleanAndUpsertTask:
    @patch("src.database_connections.db_utils.get_session")
    def test_cleans_and_upserts(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        # Mock SELECT returning no existing records (all new)
        mock_session.execute.return_value.fetchone.return_value = None

        from job_pipeline_dag import _clean_and_upsert

        ti = MagicMock()
        ti.xcom_pull.return_value = SAMPLE_RAW_JOBS

        result = _clean_and_upsert(ti=ti)

        assert result["new"] == 2
        assert result["duplicates"] == 0
        # Should push both upsert_stats and cleaned_jobs
        assert ti.xcom_push.call_count == 2

    @patch("src.database_connections.db_utils.get_session")
    def test_filters_invalid_jobs(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchone.return_value = None

        from job_pipeline_dag import _clean_and_upsert

        # Mix valid and invalid jobs
        jobs = SAMPLE_RAW_JOBS + [{"id": "bad", "description": "no title or url"}]
        ti = MagicMock()
        ti.xcom_pull.return_value = jobs

        result = _clean_and_upsert(ti=ti)

        # Only 2 valid jobs should be upserted (the bad one is filtered by clean_job_batch)
        assert result["new"] == 2


# ---------------------------------------------------------------------------
# _extract_skills
# ---------------------------------------------------------------------------
class TestExtractSkillsTask:
    def test_extracts_skills_from_cleaned_jobs(self):
        from job_pipeline_dag import _extract_skills

        ti = MagicMock()
        ti.xcom_pull.return_value = [
            {"id": "1", "description": "Python and SQL required"},
            {"id": "2", "description": "Docker and Kubernetes experience"},
        ]

        result = _extract_skills(ti=ti)

        # Should find at least some skills
        assert result > 0
        ti.xcom_pull.assert_called_once_with(task_ids="clean_and_upsert", key="cleaned_jobs")
        ti.xcom_push.assert_called_once()

    def test_empty_descriptions(self):
        from job_pipeline_dag import _extract_skills

        ti = MagicMock()
        ti.xcom_pull.return_value = [{"id": "1"}, {"id": "2"}]

        result = _extract_skills(ti=ti)
        assert result == 0


# ---------------------------------------------------------------------------
# _log_pipeline_run
# ---------------------------------------------------------------------------
class TestLogPipelineRunTask:
    @patch("src.database_connections.db_utils.get_session")
    def test_logs_successful_run(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from job_pipeline_dag import _log_pipeline_run

        ti = MagicMock()
        ti.xcom_pull.side_effect = lambda task_ids, key: {
            ("clean_and_upsert", "upsert_stats"): {"new": 5, "duplicates": 2},
            ("fetch_jobs", "raw_jobs"): SAMPLE_RAW_JOBS,
        }.get((task_ids, key))

        _log_pipeline_run(ti=ti)

        # Should execute an INSERT into scrape_runs
        mock_session.execute.assert_called_once()

    @patch("src.database_connections.db_utils.get_session")
    def test_handles_none_jobs(self, mock_get_session):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        from job_pipeline_dag import _log_pipeline_run

        ti = MagicMock()
        ti.xcom_pull.side_effect = lambda task_ids, key: {
            ("clean_and_upsert", "upsert_stats"): {"new": 0, "duplicates": 0},
            ("fetch_jobs", "raw_jobs"): None,
        }.get((task_ids, key))

        _log_pipeline_run(ti=ti)

        mock_session.execute.assert_called_once()


# ---------------------------------------------------------------------------
# DAG structure
# ---------------------------------------------------------------------------
class TestDagStructure:
    def test_dag_has_correct_task_ids(self):
        from job_pipeline_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        expected = {"fetch_jobs", "load_raw", "clean_and_upsert", "extract_skills", "log_pipeline_run"}
        assert task_ids == expected

    def test_dag_has_two_branches(self):
        from job_pipeline_dag import dag

        fetch = dag.get_task("fetch_jobs")
        downstream_ids = {t.task_id for t in fetch.downstream_list}
        assert downstream_ids == {"load_raw", "clean_and_upsert"}

    def test_clean_branch_order(self):
        from job_pipeline_dag import dag

        clean = dag.get_task("clean_and_upsert")
        downstream_ids = {t.task_id for t in clean.downstream_list}
        assert "extract_skills" in downstream_ids

        skills = dag.get_task("extract_skills")
        downstream_ids = {t.task_id for t in skills.downstream_list}
        assert "log_pipeline_run" in downstream_ids

    def test_load_raw_is_terminal(self):
        from job_pipeline_dag import dag

        load_raw = dag.get_task("load_raw")
        assert len(load_raw.downstream_list) == 0

    def test_dag_schedule(self):
        from job_pipeline_dag import dag

        assert dag.schedule_interval == "0 8 * * *"
        assert dag.catchup is False

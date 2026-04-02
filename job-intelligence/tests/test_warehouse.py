# Tests for DuckDB warehouse layer
import pytest
import duckdb
from unittest.mock import patch, MagicMock
from datetime import datetime


# ---------------------------------------------------------------------------
# init_warehouse
# ---------------------------------------------------------------------------
class TestInitWarehouse:
    def test_creates_all_tables(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import init_warehouse
        init_warehouse(con)

        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        table_names = {t[0] for t in tables}

        assert "dim_jobs" in table_names
        assert "dim_skills" in table_names
        assert "fact_job_skills" in table_names
        assert "fact_pipeline_runs" in table_names
        con.close()

    def test_idempotent(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import init_warehouse
        init_warehouse(con)
        init_warehouse(con)  # should not raise

        count = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main'"
        ).fetchone()[0]
        assert count == 4
        con.close()


# ---------------------------------------------------------------------------
# sync_jobs
# ---------------------------------------------------------------------------
class TestSyncJobs:
    @patch("src.warehouse.duckdb_loader.get_session")
    def test_syncs_jobs_from_postgres(self, mock_get_session, tmp_path):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.fetchall.return_value = [
            (1, "az_001", "http://example.com/1", "Data Engineer", "Acme",
             "London", "Build pipelines", 50000.0, 70000.0, "permanent",
             "IT Jobs", datetime(2025, 3, 1), datetime(2025, 3, 1), datetime(2025, 3, 15)),
            (2, "az_002", "http://example.com/2", "Backend Dev", "TechCo",
             "Manchester", "Write APIs", 45000.0, 65000.0, "contract",
             "Engineering", datetime(2025, 3, 2), datetime(2025, 3, 2), datetime(2025, 3, 14)),
        ]

        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import sync_jobs
        count = sync_jobs(con)

        assert count == 2
        rows = con.execute("SELECT title, company FROM dim_jobs ORDER BY job_id").fetchall()
        assert rows[0] == ("Data Engineer", "Acme")
        assert rows[1] == ("Backend Dev", "TechCo")
        con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_empty_postgres(self, mock_get_session, tmp_path):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.return_value = []

        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import sync_jobs
        count = sync_jobs(con)

        assert count == 0
        con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_full_refresh_replaces_data(self, mock_get_session, tmp_path):
        """Second sync should replace, not append."""
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import sync_jobs

        # First sync: 2 jobs
        mock_session.execute.return_value.fetchall.return_value = [
            (1, "az_001", "http://a.com", "Job A", "Co A",
             "London", "Desc", 40000.0, 60000.0, "perm", "IT",
             datetime(2025, 1, 1), datetime(2025, 1, 1), datetime(2025, 1, 1)),
            (2, "az_002", "http://b.com", "Job B", "Co B",
             "Leeds", "Desc", 30000.0, 50000.0, "perm", "IT",
             datetime(2025, 1, 2), datetime(2025, 1, 2), datetime(2025, 1, 2)),
        ]
        sync_jobs(con)

        # Second sync: only 1 job (data changed in Postgres)
        mock_session.execute.return_value.fetchall.return_value = [
            (3, "az_003", "http://c.com", "Job C", "Co C",
             "Bristol", "Desc", 55000.0, 75000.0, "contract", "Eng",
             datetime(2025, 2, 1), datetime(2025, 2, 1), datetime(2025, 2, 1)),
        ]
        count = sync_jobs(con)

        assert count == 1
        total = con.execute("SELECT COUNT(*) FROM dim_jobs").fetchone()[0]
        assert total == 1  # full refresh, not 3
        con.close()


# ---------------------------------------------------------------------------
# sync_skills
# ---------------------------------------------------------------------------
class TestSyncSkills:
    @patch("src.warehouse.duckdb_loader.get_session")
    def test_syncs_skills_and_links(self, mock_get_session, tmp_path):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # Two execute calls: skills, then job_skills
        mock_session.execute.return_value.fetchall.side_effect = [
            [(1, "python"), (2, "sql")],
            [(10, 1), (10, 2), (11, 1)],
        ]

        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import sync_skills
        count = sync_skills(con)

        assert count == 2
        links = con.execute("SELECT COUNT(*) FROM fact_job_skills").fetchone()[0]
        assert links == 3
        con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_empty_skills(self, mock_get_session, tmp_path):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.fetchall.side_effect = [[], []]

        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import sync_skills
        count = sync_skills(con)

        assert count == 0
        con.close()


# ---------------------------------------------------------------------------
# sync_pipeline_runs
# ---------------------------------------------------------------------------
class TestSyncPipelineRuns:
    @patch("src.warehouse.duckdb_loader.get_session")
    def test_syncs_runs(self, mock_get_session, tmp_path):
        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.fetchall.return_value = [
            (1, datetime(2025, 3, 15), 100, 95, 5, "success", None),
            (2, datetime(2025, 3, 16), 80, 70, 10, "success", None),
        ]

        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import sync_pipeline_runs
        count = sync_pipeline_runs(con)

        assert count == 2
        rows = con.execute("SELECT status FROM fact_pipeline_runs").fetchall()
        assert all(r[0] == "success" for r in rows)
        con.close()


# ---------------------------------------------------------------------------
# sync_all
# ---------------------------------------------------------------------------
class TestSyncAll:
    @patch("src.warehouse.duckdb_loader.sync_pipeline_runs", return_value=3)
    @patch("src.warehouse.duckdb_loader.sync_skills", return_value=10)
    @patch("src.warehouse.duckdb_loader.sync_jobs", return_value=50)
    def test_calls_all_syncs(self, mock_jobs, mock_skills, mock_runs, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)

        from src.warehouse.duckdb_loader import sync_all
        result = sync_all(con)

        assert result["jobs"] == 50
        assert result["skills"] == 10
        assert result["pipeline_runs"] == 3
        assert "synced_at" in result
        mock_jobs.assert_called_once_with(con)
        mock_skills.assert_called_once_with(con)
        mock_runs.assert_called_once_with(con)
        con.close()

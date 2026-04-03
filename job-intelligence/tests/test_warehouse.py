# Tests for DuckDB warehouse layer
import pytest
import duckdb
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_job_row(job_id, title="Dev", company="Co", last_seen=None):
    """Helper to create a mock Postgres job row tuple."""
    last_seen = last_seen or datetime(2025, 3, 15)
    return (
        job_id, f"az_{job_id}", f"http://example.com/{job_id}", title, company,
        "London", "Description", 50000.0, 70000.0, "permanent", "IT Jobs",
        datetime(2025, 3, 1), datetime(2025, 3, 1), last_seen,
    )


def _make_run_row(run_id, run_date=None):
    """Helper to create a mock Postgres pipeline run row tuple."""
    run_date = run_date or datetime(2025, 3, 15)
    return (run_id, run_date, 100, 95, 5, "success", None)


def _mock_pg_session():
    """Create a mock PostgreSQL session context manager."""
    mock_session = MagicMock()
    mock_get_session = MagicMock()
    mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
    return mock_get_session, mock_session


# ---------------------------------------------------------------------------
# init_warehouse
# ---------------------------------------------------------------------------
class TestInitWarehouse:
    def test_creates_all_tables(self, tmp_path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
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
        assert "sync_metadata" in table_names
        con.close()

    def test_idempotent(self, tmp_path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        from src.warehouse.duckdb_loader import init_warehouse
        init_warehouse(con)
        init_warehouse(con)

        count = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main'"
        ).fetchone()[0]
        assert count == 5  # 4 data tables + sync_metadata
        con.close()


# ---------------------------------------------------------------------------
# sync_jobs — full refresh (first sync / forced)
# ---------------------------------------------------------------------------
class TestSyncJobsFullRefresh:
    @patch("src.warehouse.duckdb_loader.get_session")
    def test_first_sync_does_full_refresh(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            mock_session.execute.return_value.fetchall.return_value = [
                _make_job_row(1, "Data Engineer", "Acme"),
                _make_job_row(2, "Backend Dev", "TechCo"),
            ]

            con = duckdb.connect(str(tmp_path / "test.duckdb"))
            from src.warehouse.duckdb_loader import sync_jobs
            count = sync_jobs(con)

            assert count == 2
            rows = con.execute("SELECT title FROM dim_jobs ORDER BY job_id").fetchall()
            assert rows[0][0] == "Data Engineer"
            assert rows[1][0] == "Backend Dev"

            # Verify watermark was set
            meta = con.execute("SELECT last_sync_at FROM sync_metadata WHERE table_name = 'dim_jobs'").fetchone()
            assert meta is not None
            con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_forced_full_refresh_replaces_all(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            con = duckdb.connect(str(tmp_path / "test.duckdb"))
            from src.warehouse.duckdb_loader import sync_jobs, init_warehouse
            init_warehouse(con)

            # First sync
            mock_session.execute.return_value.fetchall.return_value = [
                _make_job_row(1), _make_job_row(2),
            ]
            sync_jobs(con)

            # Forced full refresh with different data
            mock_session.execute.return_value.fetchall.return_value = [
                _make_job_row(3, "New Job"),
            ]
            count = sync_jobs(con, force_full_refresh=True)

            assert count == 1
            total = con.execute("SELECT COUNT(*) FROM dim_jobs").fetchone()[0]
            assert total == 1  # replaced, not appended
            con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_empty_postgres(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            mock_session.execute.return_value.fetchall.return_value = []
            con = duckdb.connect(str(tmp_path / "test.duckdb"))

            from src.warehouse.duckdb_loader import sync_jobs
            count = sync_jobs(con)

            assert count == 0
            con.close()


# ---------------------------------------------------------------------------
# sync_jobs — incremental
# ---------------------------------------------------------------------------
class TestSyncJobsIncremental:
    @patch("src.warehouse.duckdb_loader.get_session")
    def test_incremental_adds_new_rows(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            con = duckdb.connect(str(tmp_path / "test.duckdb"))
            from src.warehouse.duckdb_loader import sync_jobs, init_warehouse, _update_sync_metadata
            init_warehouse(con)

            # Simulate first full sync manually
            con.execute(
                "INSERT INTO dim_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(_make_job_row(1, "Old Job", "OldCo", datetime(2025, 3, 10)))
            )
            _update_sync_metadata(con, "dim_jobs", 1)

            # Incremental sync — returns only new rows
            mock_session.execute.return_value.fetchall.return_value = [
                _make_job_row(2, "New Job", "NewCo", datetime(2025, 3, 16)),
            ]
            count = sync_jobs(con)

            assert count == 1
            total = con.execute("SELECT COUNT(*) FROM dim_jobs").fetchone()[0]
            assert total == 2  # old row preserved + new row added
            con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_incremental_updates_existing_rows(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            con = duckdb.connect(str(tmp_path / "test.duckdb"))
            from src.warehouse.duckdb_loader import sync_jobs, init_warehouse, _update_sync_metadata
            init_warehouse(con)

            # Pre-existing row
            con.execute(
                "INSERT INTO dim_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(_make_job_row(1, "Old Title", "OldCo", datetime(2025, 3, 10)))
            )
            _update_sync_metadata(con, "dim_jobs", 1)

            # Same job_id comes back with updated data
            mock_session.execute.return_value.fetchall.return_value = [
                _make_job_row(1, "Updated Title", "NewCo", datetime(2025, 3, 16)),
            ]
            count = sync_jobs(con)

            assert count == 1
            total = con.execute("SELECT COUNT(*) FROM dim_jobs").fetchone()[0]
            assert total == 1  # updated in place, not duplicated
            title = con.execute("SELECT title FROM dim_jobs WHERE job_id = 1").fetchone()[0]
            assert title == "Updated Title"
            con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_incremental_no_changes(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            con = duckdb.connect(str(tmp_path / "test.duckdb"))
            from src.warehouse.duckdb_loader import sync_jobs, init_warehouse, _update_sync_metadata
            init_warehouse(con)

            con.execute(
                "INSERT INTO dim_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(_make_job_row(1))
            )
            _update_sync_metadata(con, "dim_jobs", 1)

            # No rows changed since last sync
            mock_session.execute.return_value.fetchall.return_value = []
            count = sync_jobs(con)

            assert count == 0
            total = con.execute("SELECT COUNT(*) FROM dim_jobs").fetchone()[0]
            assert total == 1  # existing data untouched
            con.close()


# ---------------------------------------------------------------------------
# sync_skills (always full refresh)
# ---------------------------------------------------------------------------
class TestSyncSkills:
    @patch("src.warehouse.duckdb_loader.get_session")
    def test_syncs_skills_and_links(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            mock_session.execute.return_value.fetchall.side_effect = [
                [(1, "python"), (2, "sql")],
                [(10, 1), (10, 2), (11, 1)],
            ]

            con = duckdb.connect(str(tmp_path / "test.duckdb"))
            from src.warehouse.duckdb_loader import sync_skills
            count = sync_skills(con)

            assert count == 2
            links = con.execute("SELECT COUNT(*) FROM fact_job_skills").fetchone()[0]
            assert links == 3
            con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_empty_skills(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            mock_session.execute.return_value.fetchall.side_effect = [[], []]
            con = duckdb.connect(str(tmp_path / "test.duckdb"))

            from src.warehouse.duckdb_loader import sync_skills
            count = sync_skills(con)

            assert count == 0
            con.close()


# ---------------------------------------------------------------------------
# sync_pipeline_runs — incremental
# ---------------------------------------------------------------------------
class TestSyncPipelineRuns:
    @patch("src.warehouse.duckdb_loader.get_session")
    def test_first_sync_full_refresh(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            mock_session.execute.return_value.fetchall.return_value = [
                _make_run_row(1), _make_run_row(2),
            ]
            con = duckdb.connect(str(tmp_path / "test.duckdb"))

            from src.warehouse.duckdb_loader import sync_pipeline_runs
            count = sync_pipeline_runs(con)

            assert count == 2
            con.close()

    @patch("src.warehouse.duckdb_loader.get_session")
    def test_incremental_appends_new_runs(self, mock_get_session, tmp_path):
        mock_get_session, mock_session = _mock_pg_session()

        with patch("src.warehouse.duckdb_loader.get_session", mock_get_session):
            con = duckdb.connect(str(tmp_path / "test.duckdb"))
            from src.warehouse.duckdb_loader import sync_pipeline_runs, init_warehouse, _update_sync_metadata
            init_warehouse(con)

            # Pre-existing run
            con.execute(
                "INSERT INTO fact_pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                list(_make_run_row(1, datetime(2025, 3, 10)))
            )
            _update_sync_metadata(con, "fact_pipeline_runs", 1)

            # New run since last sync
            mock_session.execute.return_value.fetchall.return_value = [
                _make_run_row(2, datetime(2025, 3, 16)),
            ]
            count = sync_pipeline_runs(con)

            assert count == 1
            total = con.execute("SELECT COUNT(*) FROM fact_pipeline_runs").fetchone()[0]
            assert total == 2  # old + new
            con.close()


# ---------------------------------------------------------------------------
# sync_metadata helpers
# ---------------------------------------------------------------------------
class TestSyncMetadata:
    def test_get_last_sync_returns_none_initially(self, tmp_path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        from src.warehouse.duckdb_loader import init_warehouse, _get_last_sync
        init_warehouse(con)

        assert _get_last_sync(con, "dim_jobs") is None
        con.close()

    def test_update_and_get_watermark(self, tmp_path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        from src.warehouse.duckdb_loader import init_warehouse, _get_last_sync, _update_sync_metadata
        init_warehouse(con)

        _update_sync_metadata(con, "dim_jobs", 50)
        result = _get_last_sync(con, "dim_jobs")
        assert result is not None

        # Update again — should overwrite, not duplicate
        _update_sync_metadata(con, "dim_jobs", 100)
        count = con.execute(
            "SELECT COUNT(*) FROM sync_metadata WHERE table_name = 'dim_jobs'"
        ).fetchone()[0]
        assert count == 1
        con.close()


# ---------------------------------------------------------------------------
# sync_all
# ---------------------------------------------------------------------------
class TestSyncAll:
    @patch("src.warehouse.duckdb_loader.sync_pipeline_runs", return_value=3)
    @patch("src.warehouse.duckdb_loader.sync_skills", return_value=10)
    @patch("src.warehouse.duckdb_loader.sync_jobs", return_value=50)
    def test_calls_all_syncs(self, mock_jobs, mock_skills, mock_runs, tmp_path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))

        from src.warehouse.duckdb_loader import sync_all
        result = sync_all(con)

        assert result["jobs"] == 50
        assert result["skills"] == 10
        assert result["pipeline_runs"] == 3
        assert result["strategy"] == "incremental"
        assert "synced_at" in result
        con.close()

    @patch("src.warehouse.duckdb_loader.sync_pipeline_runs", return_value=5)
    @patch("src.warehouse.duckdb_loader.sync_skills", return_value=8)
    @patch("src.warehouse.duckdb_loader.sync_jobs", return_value=100)
    def test_force_full_refresh_passes_through(self, mock_jobs, mock_skills, mock_runs, tmp_path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))

        from src.warehouse.duckdb_loader import sync_all
        result = sync_all(con, force_full_refresh=True)

        assert result["strategy"] == "full_refresh"
        mock_jobs.assert_called_once_with(con, force_full_refresh=True)
        mock_runs.assert_called_once_with(con, force_full_refresh=True)
        con.close()

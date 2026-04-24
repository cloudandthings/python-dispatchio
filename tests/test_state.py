"""Tests for the SQLAlchemy state store implementation."""

from __future__ import annotations

from uuid import uuid4

from dispatchio.models import Attempt, Status
from dispatchio.state import StateStore
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore


def _make_attempt(
    job_name="job",
    run_key="20250115",
    attempt=0,
    status=Status.DONE,
    correlation_id=None,
    **kw,
):
    if correlation_id is None:
        correlation_id = uuid4()
    return Attempt(
        job_name=job_name,
        run_key=run_key,
        attempt=attempt,
        correlation_id=correlation_id,
        status=status,
        **kw,
    )


# ---------------------------------------------------------------------------
# Shared behaviour — run the same tests against both stores
# ---------------------------------------------------------------------------


class SharedStateStoreBehaviour:
    """Mix-in. Subclasses provide self.store."""

    store: StateStore

    def test_get_latest_attempt_returns_none_for_missing(self):
        assert self.store.get_latest_attempt("no_job", "99991231") is None

    def test_append_and_get_latest_attempt_roundtrip(self):
        record = _make_attempt()
        self.store.append_attempt(record)
        got = self.store.get_latest_attempt("job", "20250115")
        assert got is not None
        assert got.status == Status.DONE

    def test_get_attempt_by_id(self):
        correlation_id = uuid4()
        record = _make_attempt(correlation_id=correlation_id)
        self.store.append_attempt(record)
        got = self.store.get_attempt(correlation_id)
        assert got is not None
        assert got.correlation_id == correlation_id

    def test_update_attempt_modifies_existing(self):
        correlation_id = uuid4()
        record = _make_attempt(correlation_id=correlation_id, status=Status.RUNNING)
        self.store.append_attempt(record)
        updated = record.model_copy(update={"status": Status.DONE})
        self.store.update_attempt(updated)
        got = self.store.get_attempt(correlation_id)
        assert got.status == Status.DONE

    def test_list_all_attempts(self):
        self.store.append_attempt(_make_attempt(job_name="a", run_key="1"))
        self.store.append_attempt(_make_attempt(job_name="b", run_key="1"))
        records = self.store.list_attempts()
        assert len(records) == 2

    def test_list_filter_by_job_name(self):
        self.store.append_attempt(_make_attempt(job_name="a", run_key="1"))
        self.store.append_attempt(_make_attempt(job_name="b", run_key="1"))
        records = self.store.list_attempts(job_name="a")
        assert len(records) == 1
        assert records[0].job_name == "a"

    def test_list_filter_by_run_key(self):
        self.store.append_attempt(_make_attempt(run_key="1"))
        self.store.append_attempt(_make_attempt(run_key="2"))
        records = self.store.list_attempts(run_key="1")
        assert len(records) == 1

    def test_list_filter_by_status(self):
        self.store.append_attempt(
            _make_attempt(job_name="a", run_key="1", status=Status.DONE)
        )
        self.store.append_attempt(
            _make_attempt(job_name="b", run_key="1", status=Status.ERROR)
        )
        records = self.store.list_attempts(status=Status.DONE)
        assert len(records) == 1
        assert records[0].job_name == "a"

    def test_list_sorted_by_attempt_desc(self):
        # Test that latest attempt is returned first
        self.store.append_attempt(_make_attempt(job_name="a", run_key="1", attempt=0))
        self.store.append_attempt(_make_attempt(job_name="a", run_key="1", attempt=1))
        records = self.store.list_attempts(job_name="a", run_key="1")
        assert records[0].attempt == 1  # latest first
        assert records[1].attempt == 0

    def test_trace_preserved(self):
        record = _make_attempt(trace={"executor": {"pid": 1234}})
        self.store.append_attempt(record)
        got = self.store.get_latest_attempt("job", "20250115")
        assert got.trace["executor"]["pid"] == 1234


class TestSQLAlchemyStateStore(SharedStateStoreBehaviour):
    def setup_method(self):
        self.store = SQLAlchemyStateStore("sqlite:///:memory:")

    def test_attempts_persist_across_instances_on_disk(self, tmp_path):
        """A new store instance pointing at the same file DB should see the data."""
        db_path = tmp_path / "test.db"
        store1 = SQLAlchemyStateStore(f"sqlite:///{db_path}")
        record = _make_attempt()
        store1.append_attempt(record)
        store2 = SQLAlchemyStateStore(f"sqlite:///{db_path}")
        got = store2.get_latest_attempt("job", "20250115")
        assert got is not None
        assert got.status == Status.DONE

    def test_multiple_attempts_for_same_run_key(self):
        """Test that we can have multiple attempts for the same (job, run_key)."""
        correlation_id1 = uuid4()
        correlation_id2 = uuid4()
        self.store.append_attempt(
            _make_attempt(
                job_name="job",
                run_key="20250115",
                attempt=0,
                correlation_id=correlation_id1,
                status=Status.ERROR,
            )
        )
        self.store.append_attempt(
            _make_attempt(
                job_name="job",
                run_key="20250115",
                attempt=1,
                correlation_id=correlation_id2,
                status=Status.DONE,
            )
        )
        # Get latest should return attempt 1
        latest = self.store.get_latest_attempt("job", "20250115")
        assert latest.attempt == 1
        # List should show both
        all_attempts = self.store.list_attempts(job_name="job", run_key="20250115")
        assert len(all_attempts) == 2

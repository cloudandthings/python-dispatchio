"""Tests for the SQLAlchemy state store implementation."""

from __future__ import annotations


from dispatchio.models import RunRecord, Status
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore


def _make_record(job_name="job", run_id="20250115", status=Status.DONE, **kw):
    return RunRecord(job_name=job_name, run_id=run_id, status=status, **kw)


# ---------------------------------------------------------------------------
# Shared behaviour — run the same tests against both stores
# ---------------------------------------------------------------------------


class SharedStateStoreBehaviour:
    """Mix-in. Subclasses provide self.store."""

    def test_get_returns_none_for_missing(self):
        assert self.store.get("no_job", "99991231") is None

    def test_put_and_get_roundtrip(self):
        record = _make_record()
        self.store.put(record)
        got = self.store.get("job", "20250115")
        assert got is not None
        assert got.status == Status.DONE

    def test_put_overwrites_existing(self):
        self.store.put(_make_record(status=Status.RUNNING))
        self.store.put(_make_record(status=Status.DONE))
        got = self.store.get("job", "20250115")
        assert got.status == Status.DONE

    def test_list_all_records(self):
        self.store.put(_make_record(job_name="a", run_id="1"))
        self.store.put(_make_record(job_name="b", run_id="1"))
        records = self.store.list_records()
        assert len(records) == 2

    def test_list_filter_by_job_name(self):
        self.store.put(_make_record(job_name="a", run_id="1"))
        self.store.put(_make_record(job_name="b", run_id="1"))
        records = self.store.list_records(job_name="a")
        assert len(records) == 1
        assert records[0].job_name == "a"

    def test_list_filter_by_status(self):
        self.store.put(_make_record(job_name="a", run_id="1", status=Status.DONE))
        self.store.put(_make_record(job_name="b", run_id="1", status=Status.ERROR))
        records = self.store.list_records(status=Status.DONE)
        assert len(records) == 1
        assert records[0].job_name == "a"

    def test_list_sorted(self):
        self.store.put(_make_record(job_name="z", run_id="1"))
        self.store.put(_make_record(job_name="a", run_id="1"))
        records = self.store.list_records()
        assert records[0].job_name == "a"
        assert records[1].job_name == "z"

    def test_metadata_preserved(self):
        record = _make_record(metadata={"rows": 1000})
        self.store.put(record)
        got = self.store.get("job", "20250115")
        assert got.metadata["rows"] == 1000


class TestSQLAlchemyStateStore(SharedStateStoreBehaviour):
    def setup_method(self):
        self.store = SQLAlchemyStateStore("sqlite:///:memory:")

    def test_records_persist_across_instances_on_disk(self, tmp_path):
        """A new store instance pointing at the same file DB should see the data."""
        db_path = tmp_path / "test.db"
        store1 = SQLAlchemyStateStore(f"sqlite:///{db_path}")
        store1.put(_make_record())
        store2 = SQLAlchemyStateStore(f"sqlite:///{db_path}")
        assert store2.get("job", "20250115") is not None

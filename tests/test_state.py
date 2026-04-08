"""Tests for state store implementations (memory and filesystem)."""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dispatchio.models import RunRecord, Status
from dispatchio.state.filesystem import FilesystemStateStore
from dispatchio.state.memory import MemoryStateStore


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

    def test_heartbeat_creates_running_record(self):
        now = datetime(2025, 1, 15, 3, 0, tzinfo=timezone.utc)
        self.store.heartbeat("job", "20250115", at=now)
        record = self.store.get("job", "20250115")
        assert record is not None
        assert record.status == Status.RUNNING
        assert record.last_heartbeat_at == now

    def test_heartbeat_updates_existing_record(self):
        self.store.put(_make_record(status=Status.RUNNING))
        t1 = datetime(2025, 1, 15, 3, 0, tzinfo=timezone.utc)
        t2 = datetime(2025, 1, 15, 3, 5, tzinfo=timezone.utc)
        self.store.heartbeat("job", "20250115", at=t1)
        self.store.heartbeat("job", "20250115", at=t2)
        record = self.store.get("job", "20250115")
        assert record.last_heartbeat_at == t2

    def test_metadata_preserved(self):
        record = _make_record(metadata={"rows": 1000})
        self.store.put(record)
        got = self.store.get("job", "20250115")
        assert got.metadata["rows"] == 1000


class TestMemoryStateStore(SharedStateStoreBehaviour):
    def setup_method(self):
        self.store = MemoryStateStore()


class TestFilesystemStateStore(SharedStateStoreBehaviour):
    def setup_method(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.store = FilesystemStateStore(self._tmpdir.name)

    def teardown_method(self):
        self._tmpdir.cleanup()

    def test_records_persist_across_instances(self):
        """A new store instance pointing at the same directory should see the data."""
        self.store.put(_make_record())
        store2 = FilesystemStateStore(self._tmpdir.name)
        assert store2.get("job", "20250115") is not None

    def test_atomic_write_file_exists(self):
        self.store.put(_make_record())
        path = Path(self._tmpdir.name) / "job" / "20250115.json"
        assert path.exists()

    def test_skips_corrupt_files(self):
        """Corrupt JSON files should be silently skipped during list."""
        job_dir = Path(self._tmpdir.name) / "job"
        job_dir.mkdir()
        (job_dir / "bad.json").write_text("not json at all {{{")
        # Should not raise
        records = self.store.list_records()
        assert records == []

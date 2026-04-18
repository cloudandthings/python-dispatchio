"""Tests for the CLI commands."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from dispatchio.cli.main import cli
from dispatchio.models import Status
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore


@pytest.fixture()
def store():
    return SQLAlchemyStateStore("sqlite:///:memory:")


def _invoke_record_set(store, job_name, run_id, status, reason=None):
    runner = CliRunner()
    args = ["record", "set", job_name, run_id, status]
    if reason:
        args += ["--reason", reason]
    with patch("dispatchio.cli.main._load_store_from_context", return_value=store):
        return runner.invoke(cli, args)


class TestRecordSet:
    def test_creates_new_record_when_none_exists(self, store):
        """record set must not crash when no prior record exists (bug: missing required fields)."""
        result = _invoke_record_set(store, "my_job", "20250115", "done")

        assert result.exit_code == 0, result.output
        record = store.get_latest_attempt("my_job", "20250115")
        assert record is not None
        assert record.status == Status.DONE
        assert record.attempt == 0
        assert record.dispatchio_attempt_id is not None

    def test_updates_existing_record(self, store):
        """record set updates status when a record already exists."""
        from uuid import uuid4
        from dispatchio.models import AttemptRecord

        existing = AttemptRecord(
            job_name="my_job",
            logical_run_id="20250115",
            attempt=0,
            dispatchio_attempt_id=uuid4(),
            status=Status.RUNNING,
        )
        store.append_attempt(existing)

        result = _invoke_record_set(store, "my_job", "20250115", "done")

        assert result.exit_code == 0, result.output
        record = store.get_latest_attempt("my_job", "20250115")
        assert record.status == Status.DONE

    def test_sets_reason_on_new_record(self, store):
        result = _invoke_record_set(
            store, "my_job", "20250115", "error", reason="disk full"
        )

        assert result.exit_code == 0, result.output
        record = store.get_latest_attempt("my_job", "20250115")
        assert record.status == Status.ERROR
        assert record.reason == "disk full"

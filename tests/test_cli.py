"""Tests for the Typer CLI commands."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from dispatchio.cli.main import app
from dispatchio.models import Status
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore


runner = CliRunner()


@pytest.fixture()
def store() -> SQLAlchemyStateStore:
    return SQLAlchemyStateStore("sqlite:///:memory:")


def _invoke_record_set(
    store: SQLAlchemyStateStore,
    job_name: str,
    run_id: str,
    status: str,
    reason: str | None = None,
):
    args = ["record", "set", job_name, run_id, status]
    if reason:
        args += ["--reason", reason]
    with patch("dispatchio.cli.main._load_store_from_context", return_value=store):
        return runner.invoke(app, args)


class TestRecordSet:
    def test_creates_new_record_when_none_exists(self, store: SQLAlchemyStateStore):
        """record set must not crash when no prior record exists (bug: missing required fields)."""
        result = _invoke_record_set(store, "my_job", "20250115", "done")

        assert result.exit_code == 0, result.output
        record = store.get_latest_attempt("my_job", "20250115")
        assert record is not None
        assert record.status == Status.DONE
        assert record.attempt == 0
        assert record.dispatchio_attempt_id is not None

    def test_updates_existing_record(self, store: SQLAlchemyStateStore):
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

    def test_sets_reason_on_new_record(self, store: SQLAlchemyStateStore):
        result = _invoke_record_set(
            store, "my_job", "20250115", "error", reason="disk full"
        )

        assert result.exit_code == 0, result.output
        record = store.get_latest_attempt("my_job", "20250115")
        assert record.status == Status.ERROR
        assert record.reason == "disk full"


def test_help_lists_expected_top_level_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0, result.output
    for command_name in [
        "tick",
        "run",
        "status",
        "ticks",
        "cancel",
        "record",
        "retry",
        "context",
        "graph",
    ]:
        assert command_name in result.output



def test_tick_requires_orchestrator() -> None:
    result = runner.invoke(app, ["tick"])

    assert result.exit_code == 1, result.output
    assert "Specify an orchestrator" in result.output



def test_status_shows_empty_message(store: SQLAlchemyStateStore) -> None:
    with patch("dispatchio.cli.main._load_store_from_context", return_value=store):
        result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    assert "No records found." in result.output



def test_context_list_shows_empty_message() -> None:
    class FakeContextStore:
        def list(self) -> list[object]:
            return []

        def current_name(self) -> None:
            return None

    with patch("dispatchio.contexts.ContextStore", return_value=FakeContextStore()):
        result = runner.invoke(app, ["context", "list"])

    assert result.exit_code == 0, result.output
    assert "No contexts registered." in result.output


def test_run_requires_entry_point_or_script() -> None:
    result = runner.invoke(app, ["run"])

    assert result.exit_code == 1, result.output
    assert "ENTRY_POINT" in result.output or "entry_point" in result.output.lower() or "--script" in result.output


def test_run_rejects_malformed_entry_point() -> None:
    result = runner.invoke(app, ["run", "no_colon_here"])

    assert result.exit_code == 1, result.output
    assert "module:function" in result.output


def test_cancel_requires_run_id_and_job() -> None:
    result = runner.invoke(app, ["cancel"])

    # Typer/Click raises UsageError for missing required options before our code runs
    assert result.exit_code != 0


def test_record_set_subcommand_help() -> None:
    result = runner.invoke(app, ["record", "set", "--help"])

    assert result.exit_code == 0, result.output
    assert "JOB_NAME" in result.output
    assert "RUN_ID" in result.output
    assert "STATUS" in result.output

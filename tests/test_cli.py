"""Tests for the Typer CLI commands."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib
from unittest.mock import patch
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from dispatchio.cli import output
from dispatchio.cli.app import app
from dispatchio.cli.app import callback as cli_callback
from dispatchio.cli.errors import CliUserError
from dispatchio.graph import GraphValidationError
from dispatchio.models import (
    AttemptRecord,
    JobAction,
    JobTickResult,
    RetryRequest,
    Status,
    TickResult,
)
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
    args = ["record", "set", job_name, run_id, status, "--yes"]
    if reason:
        args += ["--reason", reason]
    with patch("dispatchio.cli.record.load_store_from_context", return_value=store):
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
        "run-script",
        "status",
        "ticks",
        "cancel",
        "record",
        "retry",
        "context",
        "graph",
    ]:
        assert command_name in result.output


def test_tick_requires_orchestrator(rich_error_output) -> None:
    result = runner.invoke(app, ["tick"])

    assert result.exit_code == 1, result.output
    assert "Specify an orchestrator" in rich_error_output.export_text()


def test_status_shows_empty_message(store: SQLAlchemyStateStore, rich_output) -> None:
    with patch("dispatchio.cli.root.load_store_from_context", return_value=store):
        result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    assert "No records found." in rich_output.export_text()


def test_context_list_shows_empty_message(rich_output) -> None:
    class FakeContextStore:
        def list(self) -> list[object]:
            return []

        def current_name(self) -> None:
            return None

    with patch("dispatchio.contexts.ContextStore", return_value=FakeContextStore()):
        result = runner.invoke(app, ["context", "list"])

    assert result.exit_code == 0, result.output
    assert "No contexts registered." in rich_output.export_text()


def test_run_requires_entry_point() -> None:
    result = runner.invoke(app, ["run"])

    assert result.exit_code != 0, result.output
    assert "Missing argument" in result.output


def test_run_rejects_malformed_entry_point(rich_error_output) -> None:
    result = runner.invoke(app, ["run", "no_colon_here"])

    assert result.exit_code == 1, result.output
    assert "module:function" in rich_error_output.export_text()


def test_run_script_requires_function_name() -> None:
    result = runner.invoke(app, ["run-script", "tests/test_cli.py"])

    assert result.exit_code != 0, result.output
    assert "Missing argument" in result.output


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


def test_handle_cli_errors_prints_user_error(rich_error_output) -> None:
    with patch(
        "dispatchio.cli.root.load_orchestrator",
        side_effect=CliUserError("boom"),
    ):
        result = runner.invoke(app, ["tick", "--orchestrator", "a:b"])

    assert result.exit_code == 1
    assert "boom" in rich_error_output.export_text()


def test_tick_dry_run_plans_without_submission(rich_output) -> None:
    called_kwargs: dict[str, object] = {}

    fake_orch = type("FakeOrchestrator", (), {})()
    fake_orch.admission_policy = type(
        "AdmissionPolicyStub", (), {"pools": {"default": object()}}
    )()

    def _tick(**kwargs):
        called_kwargs.update(kwargs)
        return TickResult(
            reference_time=datetime(2025, 1, 15, tzinfo=timezone.utc),
            results=[
                JobTickResult(
                    job_name="job_a",
                    run_id="20250115",
                    action=JobAction.WOULD_SUBMIT,
                )
            ],
        )

    fake_orch.tick = _tick

    with patch("dispatchio.cli.root.load_orchestrator", return_value=fake_orch):
        result = runner.invoke(app, ["tick", "--orchestrator", "a:b", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert called_kwargs.get("dry_run") is True
    out = rich_output.export_text()
    assert "would_submit" in out


def test_tick_pool_forwards_to_orchestrator(rich_output) -> None:
    called_kwargs: dict[str, object] = {}

    fake_orch = type("FakeOrchestrator", (), {})()
    fake_orch.admission_policy = type(
        "AdmissionPolicyStub", (), {"pools": {"default": object(), "bulk": object()}}
    )()

    def _tick(**kwargs):
        called_kwargs.update(kwargs)
        return TickResult(
            reference_time=datetime(2025, 1, 15, tzinfo=timezone.utc),
            results=[],
        )

    fake_orch.tick = _tick

    with patch("dispatchio.cli.root.load_orchestrator", return_value=fake_orch):
        result = runner.invoke(app, ["tick", "--orchestrator", "a:b", "--pool", "bulk"])

    assert result.exit_code == 0, result.output
    assert called_kwargs.get("pool") == "bulk"


def test_tick_pool_rejects_unknown_pool(rich_error_output) -> None:
    fake_orch = type("FakeOrchestrator", (), {})()
    fake_orch.admission_policy = type(
        "AdmissionPolicyStub", (), {"pools": {"default": object()}}
    )()
    fake_orch.tick = lambda **kwargs: TickResult(
        reference_time=datetime(2025, 1, 15, tzinfo=timezone.utc),
        results=[],
    )

    with patch("dispatchio.cli.root.load_orchestrator", return_value=fake_orch):
        result = runner.invoke(app, ["tick", "--orchestrator", "a:b", "--pool", "bulk"])

    assert result.exit_code == 1, result.output
    assert "Unknown pool" in rich_error_output.export_text()


def test_tick_dry_run_renders_would_defer(rich_output) -> None:
    fake_orch = type("FakeOrchestrator", (), {})()
    fake_orch.admission_policy = type(
        "AdmissionPolicyStub", (), {"pools": {"default": object()}}
    )()

    fake_orch.tick = lambda **kwargs: TickResult(
        reference_time=datetime(2025, 1, 15, tzinfo=timezone.utc),
        results=[
            JobTickResult(
                job_name="job_a",
                run_id="20250115",
                action=JobAction.WOULD_DEFER,
                detail="deferred_submit_limit submit_jobs_this_tick=1/1",
            )
        ],
    )

    with patch("dispatchio.cli.root.load_orchestrator", return_value=fake_orch):
        result = runner.invoke(app, ["tick", "--orchestrator", "a:b", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "would_defer" in rich_output.export_text()


def test_status_renders_rich_table(
    store: SQLAlchemyStateStore,
    rich_output,
) -> None:
    record = AttemptRecord(
        job_name="job_a",
        logical_run_id="20250115",
        attempt=0,
        dispatchio_attempt_id=uuid4(),
        status=Status.DONE,
    )
    store.append_attempt(record)

    with patch("dispatchio.cli.root.load_store_from_context", return_value=store):
        result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "JOB" in out
    assert "RUN_ID" in out
    assert "job_a" in out


def test_retry_list_attempts_renders_rich_table(
    store: SQLAlchemyStateStore,
    rich_output,
) -> None:
    record = AttemptRecord(
        job_name="job_a",
        logical_run_id="20250115",
        attempt=1,
        dispatchio_attempt_id=uuid4(),
        status=Status.ERROR,
    )
    store.append_attempt(record)

    with patch("dispatchio.cli.retry.load_store_from_context", return_value=store):
        result = runner.invoke(app, ["retry", "list", "--filter", "attempts"])

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "ATTEMPT" in out
    assert "job_a" in out


def test_retry_list_requests_renders_rich_table(
    store: SQLAlchemyStateStore,
    rich_output,
) -> None:
    request = RetryRequest(
        requested_at=datetime.now(tz=timezone.utc),
        requested_by="cli",
        logical_run_id="20250115",
        selected_jobs=["job_a", "job_b"],
    )
    store.append_retry_request(request)

    with patch("dispatchio.cli.retry.load_store_from_context", return_value=store):
        result = runner.invoke(app, ["retry", "list", "--filter", "requests"])

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "REQUESTED_AT" in out
    assert "job_a, job_b" in out


def test_context_list_renders_rich_table(rich_output) -> None:
    class FakeContext:
        def __init__(self, name: str, config_path: str, description: str = ""):
            self.name = name
            self.config_path = config_path
            self.description = description

    class FakeContextStore:
        def list(self) -> list[object]:
            return [FakeContext("dev", "/tmp/dev.toml", "Dev context")]

        def current_name(self) -> str:
            return "dev"

    with patch("dispatchio.contexts.ContextStore", return_value=FakeContextStore()):
        result = runner.invoke(app, ["context", "list"])

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "NAME" in out
    assert "CONFIG PATH" in out
    assert "dev" in out


def test_no_color_callback_reconfigures_consoles() -> None:
    original_console = output.console
    original_error_console = output.error_console

    try:
        cli_callback(no_color=True)
        assert output.console.no_color is True
        assert output.error_console.no_color is True
    finally:
        output.console = original_console
        output.error_console = original_error_console


def test_output_console_honors_no_color_env(monkeypatch) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    reloaded_output = importlib.reload(output)

    try:
        assert reloaded_output.console.no_color is True
        assert reloaded_output.error_console.no_color is True
    finally:
        monkeypatch.delenv("NO_COLOR", raising=False)
        importlib.reload(output)


def test_graph_validate_reports_load_failure(tmp_path, rich_error_output) -> None:
    graph_path = tmp_path / "graph.json"
    graph_path.write_text("{}")

    with patch(
        "dispatchio.graph.load_graph",
        side_effect=GraphValidationError(["invalid graph artifact"]),
    ):
        result = runner.invoke(app, ["graph", "validate", str(graph_path)])

    assert result.exit_code == 1, result.output
    error_text = rich_error_output.export_text()
    assert "Graph validation failed" in error_text
    assert "invalid graph artifact" in error_text


def test_graph_validate_reports_validation_failure(tmp_path, rich_error_output) -> None:
    graph_path = tmp_path / "graph.json"
    graph_path.write_text("{}")

    with patch("dispatchio.graph.load_graph", return_value=object()):
        with patch(
            "dispatchio.graph.validate_graph",
            side_effect=GraphValidationError(["graph validation failed"]),
        ):
            result = runner.invoke(app, ["graph", "validate", str(graph_path)])

    assert result.exit_code == 1, result.output
    error_text = rich_error_output.export_text()
    assert "Graph validation failed" in error_text
    assert "graph validation failed" in error_text


def test_record_set_prompts_without_yes(store: SQLAlchemyStateStore) -> None:
    with patch("dispatchio.cli.record.load_store_from_context", return_value=store):
        with patch(
            "dispatchio.cli.record.output.confirm", return_value=True
        ) as confirm_mock:
            result = runner.invoke(app, ["record", "set", "job_a", "20250115", "done"])

    assert result.exit_code == 0, result.output
    confirm_mock.assert_called_once()


def test_record_set_yes_skips_prompt(store: SQLAlchemyStateStore) -> None:
    with patch("dispatchio.cli.record.load_store_from_context", return_value=store):
        with patch("dispatchio.cli.record.output.confirm") as confirm_mock:
            result = runner.invoke(
                app, ["record", "set", "job_a", "20250115", "done", "--yes"]
            )

    assert result.exit_code == 0, result.output
    confirm_mock.assert_not_called()


def test_cancel_prompts_without_yes(store: SQLAlchemyStateStore) -> None:
    record = AttemptRecord(
        job_name="job_a",
        logical_run_id="20250115",
        attempt=0,
        dispatchio_attempt_id=uuid4(),
        status=Status.RUNNING,
    )
    store.append_attempt(record)

    with patch("dispatchio.cli.root.load_store_from_context", return_value=store):
        with patch(
            "dispatchio.cli.root.output.confirm", return_value=True
        ) as confirm_mock:
            result = runner.invoke(
                app, ["cancel", "--run-id", "20250115", "--job", "job_a"]
            )

    assert result.exit_code == 0, result.output
    confirm_mock.assert_called_once()


def test_cancel_yes_skips_prompt(store: SQLAlchemyStateStore) -> None:
    record = AttemptRecord(
        job_name="job_a",
        logical_run_id="20250115",
        attempt=0,
        dispatchio_attempt_id=uuid4(),
        status=Status.RUNNING,
    )
    store.append_attempt(record)

    with patch("dispatchio.cli.root.load_store_from_context", return_value=store):
        with patch("dispatchio.cli.root.output.confirm") as confirm_mock:
            result = runner.invoke(
                app,
                ["cancel", "--run-id", "20250115", "--job", "job_a", "--yes"],
            )

    assert result.exit_code == 0, result.output
    confirm_mock.assert_not_called()

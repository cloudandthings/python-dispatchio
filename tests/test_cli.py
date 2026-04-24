"""Tests for the Typer CLI commands."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib
from unittest.mock import patch
from uuid import uuid4
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


def _invoke_record_set(
    store: SQLAlchemyStateStore,
    job_name: str,
    run_key: str,
    status: str,
    reason: str | None = None,
):
    args = ["record", "set", job_name, run_key, status, "--yes"]
    if reason:
        args += ["--reason", reason]
    with patch("dispatchio.cli.record.load_store_from_context", return_value=store):
        return runner.invoke(app, args)


class TestRecordSet:
    def test_creates_new_record_when_none_exists(
        self, state_store: SQLAlchemyStateStore
    ):
        """record set must not crash when no prior record exists (bug: missing required fields)."""
        result = _invoke_record_set(state_store, "my_job", "20250115", "done")

        assert result.exit_code == 0, result.output
        record = state_store.get_latest_attempt("my_job", "20250115")
        assert record is not None
        assert record.status == Status.DONE
        assert record.attempt == 0
        assert record.correlation_id is not None

    def test_updates_existing_record(self, state_store: SQLAlchemyStateStore):
        """record set updates status when a record already exists."""

        existing = AttemptRecord(
            job_name="my_job",
            run_key="20250115",
            attempt=0,
            correlation_id=uuid4(),
            status=Status.RUNNING,
        )
        state_store.append_attempt(existing)

        result = _invoke_record_set(state_store, "my_job", "20250115", "done")

        assert result.exit_code == 0, result.output
        record = state_store.get_latest_attempt("my_job", "20250115")
        assert record is not None
        assert record.status == Status.DONE

    def test_sets_reason_on_new_record(self, state_store: SQLAlchemyStateStore):
        result = _invoke_record_set(
            state_store, "my_job", "20250115", "error", reason="disk full"
        )

        assert result.exit_code == 0, result.output
        record = state_store.get_latest_attempt("my_job", "20250115")
        assert record is not None
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
        "run-list",
        "run-show",
        "run-resume",
        "run-cancel",
        "backfill-plan",
        "backfill-enqueue",
        "replay-enqueue",
    ]:
        assert command_name in result.output


def test_backfill_plan_prints_run_keys(rich_output) -> None:
    fake_orch = type("FakeOrchestrator", (), {})()
    fake_orch.plan_backfill = lambda start, end: ["20250101", "20250102"]

    with patch("dispatchio.cli.root.load_orchestrator", return_value=fake_orch):
        result = runner.invoke(
            app,
            [
                "backfill-plan",
                "--orchestrator",
                "a:b",
                "--start",
                "2025-01-01T00:00:00+00:00",
                "--end",
                "2025-01-02T00:00:00+00:00",
            ],
        )

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "Planned 2 run key(s)" in out
    assert "20250101" in out


def test_run_list_prints_rows(rich_output) -> None:
    class _Run:
        def __init__(self):
            self.orchestrator_run_id = "id-1"
            self.run_key = "20250101"
            self.status = type("S", (), {"value": "pending"})()
            self.mode = type("M", (), {"value": "backfill"})()
            self.priority = 10

    fake_orch = type("FakeOrchestrator", (), {})()
    fake_orch.list_runs = lambda status=None, mode=None: [_Run()]

    with patch("dispatchio.cli.root.load_orchestrator", return_value=fake_orch):
        result = runner.invoke(app, ["run-list", "--orchestrator", "a:b"])

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "20250101" in out
    assert "pending" in out


def test_tick_requires_orchestrator(rich_error_output) -> None:
    result = runner.invoke(app, ["tick"])

    assert result.exit_code == 1, result.output
    assert "Specify an orchestrator" in rich_error_output.export_text()


def test_status_shows_empty_message(
    state_store: SQLAlchemyStateStore, rich_output
) -> None:
    with patch("dispatchio.cli.root.load_store_from_context", return_value=state_store):
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
    assert "RUN_KEY" in result.output
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
                    run_key="20250115",
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
                run_key="20250115",
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
    state_store: SQLAlchemyStateStore,
    rich_output,
) -> None:
    record = AttemptRecord(
        job_name="job_a",
        run_key="20250115",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.DONE,
    )
    state_store.append_attempt(record)

    with patch("dispatchio.cli.root.load_store_from_context", return_value=state_store):
        result = runner.invoke(app, ["status"])

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "JOB" in out
    assert "RUN_KEY" in out
    assert "job_a" in out


def test_retry_list_attempts_renders_rich_table(
    state_store: SQLAlchemyStateStore,
    rich_output,
) -> None:
    record = AttemptRecord(
        job_name="job_a",
        run_key="20250115",
        attempt=1,
        correlation_id=uuid4(),
        status=Status.ERROR,
    )
    state_store.append_attempt(record)

    with patch(
        "dispatchio.cli.retry.load_store_from_context", return_value=state_store
    ):
        result = runner.invoke(app, ["retry", "list", "--filter", "attempts"])

    assert result.exit_code == 0, result.output
    out = rich_output.export_text()
    assert "ATTEMPT" in out
    assert "job_a" in out


def test_retry_list_requests_renders_rich_table(
    state_store: SQLAlchemyStateStore,
    rich_output,
) -> None:
    request = RetryRequest(
        requested_at=datetime.now(tz=timezone.utc),
        requested_by="cli",
        run_key="20250115",
        selected_jobs=["job_a", "job_b"],
    )
    state_store.append_retry_request(request)

    with patch(
        "dispatchio.cli.retry.load_store_from_context", return_value=state_store
    ):
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


def test_record_set_prompts_without_yes(state_store: SQLAlchemyStateStore) -> None:
    with patch(
        "dispatchio.cli.record.load_store_from_context", return_value=state_store
    ):
        with patch(
            "dispatchio.cli.record.output.confirm", return_value=True
        ) as confirm_mock:
            result = runner.invoke(app, ["record", "set", "job_a", "20250115", "done"])

    assert result.exit_code == 0, result.output
    confirm_mock.assert_called_once()


def test_record_set_yes_skips_prompt(state_store: SQLAlchemyStateStore) -> None:
    with patch(
        "dispatchio.cli.record.load_store_from_context", return_value=state_store
    ):
        with patch("dispatchio.cli.record.output.confirm") as confirm_mock:
            result = runner.invoke(
                app, ["record", "set", "job_a", "20250115", "done", "--yes"]
            )

    assert result.exit_code == 0, result.output
    confirm_mock.assert_not_called()


def test_cancel_prompts_without_yes(state_store: SQLAlchemyStateStore) -> None:
    record = AttemptRecord(
        job_name="job_a",
        run_key="20250115",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.RUNNING,
    )
    state_store.append_attempt(record)

    with patch("dispatchio.cli.root.load_store_from_context", return_value=state_store):
        with patch(
            "dispatchio.cli.root.output.confirm", return_value=True
        ) as confirm_mock:
            result = runner.invoke(
                app, ["cancel", "--run-key", "20250115", "--job", "job_a"]
            )

    assert result.exit_code == 0, result.output
    confirm_mock.assert_called_once()


def test_cancel_yes_skips_prompt(state_store: SQLAlchemyStateStore) -> None:
    record = AttemptRecord(
        job_name="job_a",
        run_key="20250115",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.RUNNING,
    )
    state_store.append_attempt(record)

    with patch("dispatchio.cli.root.load_store_from_context", return_value=state_store):
        with patch("dispatchio.cli.root.output.confirm") as confirm_mock:
            result = runner.invoke(
                app,
                ["cancel", "--run-key", "20250115", "--job", "job_a", "--yes"],
            )

    assert result.exit_code == 0, result.output
    confirm_mock.assert_not_called()


class TestAllNamespacesFlag:
    """--all-namespaces passes namespace=None so all records are visible."""

    def _make_stores(self):
        """Return two scoped stores and one unscoped store backed by the same SQLite DB."""
        ns_a = SQLAlchemyStateStore(
            connection_string="sqlite:///:memory:", namespace="ns_a"
        )

        # Share the underlying engine so all stores hit the same in-memory DB.
        def _clone(namespace):
            s = SQLAlchemyStateStore.__new__(SQLAlchemyStateStore)
            s._engine = ns_a._engine
            s._Session = ns_a._Session
            s._lock = ns_a._lock
            s._namespace = namespace
            return s

        return ns_a, _clone("ns_b"), _clone(None)

    def test_status_all_namespaces_shows_namespace_column(self, rich_output) -> None:
        ns_a, ns_b, unscoped = self._make_stores()

        ns_a.append_attempt(
            AttemptRecord(
                job_name="job_a",
                run_key="20250115",
                attempt=0,
                correlation_id=uuid4(),
                status=Status.DONE,
            )
        )
        ns_b.append_attempt(
            AttemptRecord(
                job_name="job_b",
                run_key="20250115",
                attempt=0,
                correlation_id=uuid4(),
                status=Status.ERROR,
            )
        )

        with patch(
            "dispatchio.cli.root.load_store_from_context", return_value=unscoped
        ):
            result = runner.invoke(app, ["status", "--all-namespaces"])

        assert result.exit_code == 0, result.output
        out = rich_output.export_text()
        assert "NAMESPACE" in out
        assert "ns_a" in out
        assert "ns_b" in out
        assert "job_a" in out
        assert "job_b" in out

    def test_status_single_namespace_omits_namespace_column(
        self, state_store: SQLAlchemyStateStore, rich_output
    ) -> None:
        state_store.append_attempt(
            AttemptRecord(
                job_name="job_a",
                run_key="20250115",
                attempt=0,
                correlation_id=uuid4(),
                status=Status.DONE,
            )
        )

        with patch(
            "dispatchio.cli.root.load_store_from_context", return_value=state_store
        ):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, result.output
        assert "NAMESPACE" not in rich_output.export_text()

    def test_retry_list_all_namespaces_shows_records_from_all(
        self, rich_output
    ) -> None:
        ns_a, ns_b, unscoped = self._make_stores()

        ns_a.append_attempt(
            AttemptRecord(
                job_name="job_a",
                run_key="20250115",
                attempt=0,
                correlation_id=uuid4(),
                status=Status.ERROR,
            )
        )
        ns_b.append_attempt(
            AttemptRecord(
                job_name="job_b",
                run_key="20250115",
                attempt=0,
                correlation_id=uuid4(),
                status=Status.ERROR,
            )
        )

        with patch(
            "dispatchio.cli.retry.load_store_from_context", return_value=unscoped
        ):
            result = runner.invoke(app, ["retry", "list", "--all-namespaces"])

        assert result.exit_code == 0, result.output
        out = rich_output.export_text()
        assert "job_a" in out
        assert "job_b" in out

    def test_load_store_from_context_passes_namespace_none_when_all_namespaces(
        self,
    ) -> None:
        from dispatchio.cli.loaders import load_store_from_context

        fake_settings = type(
            "FakeSettings",
            (),
            {
                "state": type(
                    "S",
                    (),
                    {
                        "backend": "sqlalchemy",
                        "connection_string": "sqlite:///:memory:",
                        "db_echo": False,
                        "db_pool_size": 5,
                    },
                )(),
                "namespace": "prod",
            },
        )()

        with patch("dispatchio.config.loader.load_config", return_value=fake_settings):
            with patch("dispatchio.contexts.ContextStore") as MockCS:
                MockCS.return_value.resolve.return_value = None
                store = load_store_from_context(None, all_namespaces=True)

        assert store.namespace is None

    def test_load_store_from_context_passes_settings_namespace_by_default(
        self,
    ) -> None:
        from dispatchio.cli.loaders import load_store_from_context

        fake_settings = type(
            "FakeSettings",
            (),
            {
                "state": type(
                    "S",
                    (),
                    {
                        "backend": "sqlalchemy",
                        "connection_string": "sqlite:///:memory:",
                        "db_echo": False,
                        "db_pool_size": 5,
                    },
                )(),
                "namespace": "prod",
            },
        )()

        with patch("dispatchio.config.loader.load_config", return_value=fake_settings):
            with patch("dispatchio.contexts.ContextStore") as MockCS:
                MockCS.return_value.resolve.return_value = None
                store = load_store_from_context(None)

        assert store.namespace == "prod"

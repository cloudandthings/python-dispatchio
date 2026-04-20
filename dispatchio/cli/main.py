"""
Dispatchio CLI

Connects to a remote (or local) Dispatchio instance by importing the
orchestrator from a user-specified Python module path.

Configuration (in priority order):
  1. CLI flags  --orchestrator / --context
  2. Environment variables  DISPATCHIO_ORCHESTRATOR
  3. dispatchio.toml  [dispatchio] orchestrator = "mymodule:orchestrator"
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import NoReturn, Optional
from uuid import uuid4

import typer
from typing_extensions import Annotated

from dispatchio.models import AttemptRecord, Status, TickResult
from dispatchio.orchestrator import Orchestrator
from dispatchio.state import SQLAlchemyStateStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

app = typer.Typer(help="Dispatchio — lightweight tick-based batch job orchestrator.")
record_app = typer.Typer(help="Manually inspect or override run records.")
retry_app = typer.Typer(help="Create and list manual retry requests.")
context_app = typer.Typer(
    help="Manage named orchestrator contexts (config file pointers)."
)
graph_app = typer.Typer(help="Graph artifact utilities (JSON graph mode).")

app.add_typer(record_app, name="record")
app.add_typer(retry_app, name="retry")
app.add_typer(context_app, name="context")
app.add_typer(graph_app, name="graph")


def _resolve_orchestrator_path(orch_path: Optional[str]) -> Optional[str]:
    return orch_path or os.environ.get("DISPATCHIO_ORCHESTRATOR")


def _exit_with_error(message: str) -> NoReturn:
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=1)


def _load_orchestrator(path: str) -> Orchestrator:
    if ":" not in path:
        _exit_with_error(
            f"Orchestrator path must be 'module:attribute', got: {path!r}"
        )

    module_path, attr = path.rsplit(":", 1)
    sys.path.insert(0, os.getcwd())
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        _exit_with_error(f"Cannot import module {module_path!r}: {e}")

    obj = getattr(module, attr, None)
    if obj is None:
        _exit_with_error(f"Attribute {attr!r} not found in module {module_path!r}")

    if not isinstance(obj, Orchestrator):
        _exit_with_error(
            f"{path!r} is a {type(obj).__name__}, expected Orchestrator"
        )

    return obj


def _load_store_from_context(context_name: Optional[str]) -> Optional[SQLAlchemyStateStore]:
    from dispatchio.contexts import ContextStore
    from dispatchio.config.loader import _build_state, load_config

    entry = ContextStore().resolve(context_name)
    if entry is not None:
        try:
            settings = load_config(entry.config_path)
            return _build_state(settings.state)  # type: ignore[return-value]
        except Exception:
            pass

    try:
        settings = load_config()
        if settings is not None:
            return _build_state(settings.state)  # type: ignore[return-value]
    except Exception:
        pass

    return None


def _resolve_tick_log_store(context_name: Optional[str]):
    from dispatchio.contexts import ContextStore
    from dispatchio.config.loader import load_config
    from dispatchio.tick_log import FilesystemTickLogStore

    entry = ContextStore().resolve(context_name)
    if entry is not None:
        try:
            settings = load_config(entry.config_path)
            return FilesystemTickLogStore(Path(settings.state.tick_log_path))
        except Exception:
            pass

    try:
        settings = load_config()
        if settings is not None:
            return FilesystemTickLogStore(Path(settings.state.tick_log_path))
    except Exception:
        pass

    return None


OrchestratorOption = Annotated[
    Optional[str],
    typer.Option(
        "--orchestrator",
        "-o",
        help="module:attribute path to an Orchestrator, e.g. myproject.jobs:orchestrator",
    ),
]

ContextOption = Annotated[
    Optional[str],
    typer.Option(
        "--context",
        "-c",
        help="Named context from ~/.dispatchio/contexts.json. Run 'dispatchio context list' to see available contexts.",
    ),
]

ReferenceTimeOption = Annotated[
    Optional[str],
    typer.Option(
        "--reference-time",
        "-t",
        help="ISO-8601 datetime to use as the tick reference time. Defaults to now (UTC).",
    ),
]

LimitOption = Annotated[
    int,
    typer.Option("--limit", "-n", help="Max ticks to show.", show_default=True),
]

RunIdOption = Annotated[
    Optional[str],
    typer.Option("--run-id", "-r", help="Filter by run_id."),
]

JobOption = Annotated[
    Optional[str],
    typer.Option("--job", "-j", help="Filter by job name."),
]

StatusFilterOption = Annotated[
    Optional[Status],
    typer.Option("--status", help="Filter by status.", case_sensitive=False),
]


def _validate_existing_path(value: Path) -> Path:
    if not value.exists():
        raise typer.BadParameter(f"Path does not exist: {value}")
    return value


GraphPathArgument = Annotated[
    Path,
    typer.Argument(callback=_validate_existing_path, help="Path to graph artifact file."),
]


class RetryListView(str, Enum):
    ATTEMPTS = "attempts"
    REQUESTS = "requests"


_STATUS_COLOURS = {
    "done": ("green", "DONE     "),
    "submitted": ("cyan", "SUBMITTED"),
    "running": ("blue", "RUNNING  "),
    "error": ("red", "ERROR    "),
    "lost": ("yellow", "LOST     "),
    "pending": ("white", "PENDING  "),
    "skipped": ("white", "SKIPPED  "),
}

_ACTION_ICONS = {
    "submitted": "✓",
    "retrying": "↺",
    "marked_lost": "✗",
    "marked_error": "✗",
    "submission_failed": "✗",
    "skipped_condition": "·",
    "skipped_dependencies": "·",
    "skipped_already_active": "·",
    "skipped_already_done": "·",
}


def _action_icon(action: str) -> str:
    return _ACTION_ICONS.get(action, "?")


def _print_records(records: list[AttemptRecord]) -> None:
    header = f"{'JOB':<30} {'RUN_ID':<14} {'STATUS':<10} {'ATTEMPT':<8} {'COMPLETED'}"
    typer.echo(header)
    typer.echo("─" * len(header))
    for record in records:
        colour, label = _STATUS_COLOURS.get(
            record.status.value, ("white", record.status.value)
        )
        completed = record.completed_at.isoformat() if record.completed_at else "—"
        line = (
            f"{record.job_name:<30} {record.logical_run_id:<14} "
            f"{label:<10} {record.attempt:<8} {completed}"
        )
        typer.echo(typer.style(line, fg=colour))


@app.command()
def tick(
    *,
    orchestrator: OrchestratorOption = None,
    reference_time: ReferenceTimeOption = None,
) -> None:
    """Run one orchestrator tick and print results."""
    orch_path = _resolve_orchestrator_path(orchestrator)
    if not orch_path:
        _exit_with_error(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )

    orch = _load_orchestrator(orch_path)

    ref = None
    if reference_time:
        ref = datetime.fromisoformat(reference_time)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)

    result: TickResult = orch.tick(reference_time=ref)

    typer.echo(f"\nTick at {result.reference_time.isoformat()}")
    typer.echo(f"{'─' * 60}")
    if not result.results:
        typer.echo("  (no actions taken)")
    else:
        for item in result.results:
            marker = _action_icon(item.action.value)
            detail = f"  {item.detail}" if item.detail else ""
            typer.echo(
                f"  {marker} {item.job_name}[{item.run_id}] → {item.action.value}{detail}"
            )
    typer.echo()


@app.command("run")
def run_command(
    entry_point: Annotated[
        Optional[str],
        typer.Argument(help="Optional module:function entry point.", show_default=False),
    ] = None,
    *,
    script: Annotated[
        Optional[str],
        typer.Option("--script", help="Path to a Python script file."),
    ] = None,
    function_name: Annotated[
        Optional[str],
        typer.Option(
            "--function",
            help="Function name within the script (required with --script).",
        ),
    ] = None,
    job_name: Annotated[
        Optional[str],
        typer.Option(
            "--job-name",
            help="Job name override. Defaults to the function name.",
        ),
    ] = None,
) -> None:
    """
    Run a Python callable as a Dispatchio job.

    Used as the subprocess entry point for PythonJob executors. The executor
    injects DISPATCHIO_RUN_ID and reporter env vars (e.g. DISPATCHIO_DROP_DIR) so
    the job harness can complete the lifecycle without any extra arguments.

    \b
    Forms:
      dispatchio run mypackage.jobs:run_ingest          # entry_point form
      dispatchio run --script worker.py --function etl  # script form
    """
    from dispatchio.worker.harness import run_job

    if entry_point is not None:
        if ":" not in entry_point:
            _exit_with_error(
                f"entry_point must be 'module:function', got: {entry_point!r}"
            )
        module_path, func_name = entry_point.rsplit(":", 1)
        sys.path.insert(0, os.getcwd())
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError as e:
            _exit_with_error(f"Cannot import module {module_path!r}: {e}")
        fn = getattr(module, func_name, None)
        if fn is None or not callable(fn):
            _exit_with_error(
                f"Callable {func_name!r} not found in module {module_path!r}"
            )
    elif script is not None:
        if function_name is None:
            _exit_with_error("--function is required when --script is used.")
        script_path = Path(script).resolve()
        if not script_path.exists():
            _exit_with_error(f"Script not found: {script_path}")
        spec = importlib.util.spec_from_file_location("_dispatchio_job", script_path)
        module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(module)  # type: ignore[union-attr]
        fn = getattr(module, function_name, None)
        func_name = function_name
        if fn is None or not callable(fn):
            _exit_with_error(f"Function {function_name!r} not found in {script_path}")
    else:
        _exit_with_error(
            "Provide either ENTRY_POINT (module:function) or both --script and --function."
        )

    resolved_job_name = job_name or func_name
    run_job(resolved_job_name, fn)


@app.command()
def status(
    *,
    context_name: ContextOption = None,
    job: JobOption = None,
    run_id: RunIdOption = None,
    filter_status: StatusFilterOption = None,
) -> None:
    """Show the status of job runs."""
    store = _load_store_from_context(context_name)
    if store is None:
        _exit_with_error(
            "Cannot locate state store. Use --context or run from a directory with dispatchio.toml."
        )

    records = store.list_attempts(job_name=job, status=filter_status)
    if run_id:
        records = [record for record in records if record.logical_run_id == run_id]

    if not records:
        typer.echo("No records found.")
        return

    _print_records(records)


@app.command()
def ticks(
    *,
    context_name: ContextOption = None,
    limit: LimitOption = 20,
    since: Annotated[
        Optional[str], typer.Option("--since", help="Show ticks at or after this ISO timestamp.")
    ] = None,
    until: Annotated[
        Optional[str], typer.Option("--until", help="Show ticks at or before this ISO timestamp.")
    ] = None,
    detail: Annotated[
        bool, typer.Option("--detail", help="Show individual job actions.")
    ] = False,
) -> None:
    """Show tick history for this orchestrator.

    By default reads the tick log from the local dispatchio.toml.
    Use --context to inspect a different orchestrator.

    \b
    Examples:
      dispatchio ticks
      dispatchio ticks --detail
      dispatchio ticks --context weekly-reports --limit 5
      dispatchio ticks --since 2026-04-14T00:00:00
    """
    store = _resolve_tick_log_store(context_name)
    if store is None:
        _exit_with_error(
            "Cannot locate tick log. Specify --state-dir, --context, or run from a directory with dispatchio.toml."
        )

    records = store.list(limit=limit, since=since, until=until)
    if not records:
        typer.echo("No tick records found.")
        return

    for rec in records:
        submitted = sum(1 for action in rec.actions if action["action"] == "submitted")
        total = len(rec.actions)
        summary = f"{total} action(s)"
        if submitted:
            summary += f"  ({submitted} submitted)"

        typer.echo(
            f"  {rec.ticked_at}  ref={rec.reference_time[:10]}  "
            f"{rec.duration_seconds:.2f}s  {summary}"
        )

        if detail:
            for action in rec.actions:
                marker = _action_icon(action["action"])
                action_detail = f"  {action['detail']}" if action.get("detail") else ""
                typer.echo(
                    f"    {marker} {action['job_name']}[{action['run_id']}]"
                    f" → {action['action']}{action_detail}"
                )


@app.command("cancel")
def cancel(
    *,
    context_name: ContextOption = None,
    run_id: Annotated[str, typer.Option("--run-id", "-r", help="Logical run ID.")],
    job: Annotated[str, typer.Option("--job", "-j", help="Job name.")],
    attempt: Annotated[
        Optional[int],
        typer.Option("--attempt", "-a", help="Attempt number. Defaults to latest active attempt."),
    ] = None,
    reason: Annotated[
        Optional[str], typer.Option("--reason", help="Reason for cancellation.")
    ] = None,
    operator: Annotated[
        str,
        typer.Option(
            "--operator",
            help="Name/ID of the operator requesting cancellation.",
            show_default=True,
        ),
    ] = "cli",
) -> None:
    """Cancel an active attempt.

    \b
    Examples:
      dispatchio cancel --run-id 20260418 --job load_sales
      dispatchio cancel --run-id 20260418 --job load_sales --attempt 1 --reason "hung"
    """
    store = _load_store_from_context(context_name)
    if store is None:
        _exit_with_error(
            "Cannot locate state store. Use --context or run from a directory with dispatchio.toml."
        )

    if attempt is not None:
        candidates = store.list_attempts(job_name=job, logical_run_id=run_id, attempt=attempt)
        rec = candidates[0] if candidates else None
    else:
        rec = store.get_latest_attempt(job, run_id)
        if rec is not None and rec.is_finished():
            _exit_with_error(
                f"{job}[{run_id}] latest attempt {rec.attempt} is already {rec.status.value} (terminal). "
                "Specify --attempt to cancel a specific attempt."
            )

    if rec is None:
        _exit_with_error(f"No attempt found for {job}[{run_id}].")

    if rec.is_finished():
        _exit_with_error(
            f"{job}[{run_id}] attempt {rec.attempt} is already {rec.status.value} (terminal)."
        )

    cancelled = rec.model_copy(
        update={
            "status": Status.CANCELLED,
            "completed_at": datetime.now(tz=timezone.utc),
            "reason": reason,
            "operator_name": operator,
        }
    )
    store.update_attempt(cancelled)
    typer.echo(
        typer.style(
            f"Cancelled {job}[{run_id}] attempt {rec.attempt}.",
            fg="yellow",
        )
    )


@record_app.command("set")
def record_set(
    job_name: Annotated[str, typer.Argument(help="Job name.")],
    run_id: Annotated[str, typer.Argument(help="Logical run ID.")],
    new_status: Annotated[Status, typer.Argument(metavar="STATUS", help="New status value.")],
    *,
    context_name: ContextOption = None,
    reason: Annotated[
        Optional[str], typer.Option("--reason", help="Error reason string.")
    ] = None,
) -> None:
    """Manually set the status of a run record."""
    store = _load_store_from_context(context_name)
    if store is None:
        _exit_with_error(
            "Cannot locate state store. Use --context or run from a directory with dispatchio.toml."
        )
    existing = store.get_latest_attempt(job_name, run_id)
    if existing is None:
        record = AttemptRecord(
            job_name=job_name,
            logical_run_id=run_id,
            attempt=0,
            dispatchio_attempt_id=uuid4(),
            status=new_status,
            reason=reason,
        )
        store.append_attempt(record)
    else:
        record = existing.model_copy(update={"status": new_status, "reason": reason})
        store.update_attempt(record)
    typer.echo(f"Set {job_name}[{run_id}] → {new_status.value}")


@retry_app.command("create")
def retry_create(
    *,
    orchestrator: OrchestratorOption = None,
    run_id: Annotated[str, typer.Option("--run-id", "-r", help="Logical run ID to retry.")],
    jobs: Annotated[
        Optional[list[str]],
        typer.Option(
            "--jobs",
            "-j",
            help="Jobs to retry. If omitted, retries all failed jobs for the run.",
        ),
    ] = None,
    reason: Annotated[
        Optional[str], typer.Option("--reason", help="Reason for this retry.")
    ] = None,
    operator: Annotated[
        str,
        typer.Option(
            "--operator",
            help="Name/ID of the operator requesting the retry.",
            show_default=True,
        ),
    ] = "cli",
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Print selected jobs and assigned attempt numbers without writing state.",
        ),
    ] = False,
) -> None:
    """Create a manual retry for one or more jobs.

    \b
    Examples:
      dispatchio retry create --run-id 20260418
      dispatchio retry create --run-id 20260418 --jobs load_sales --jobs enrich
      dispatchio retry create --run-id 20260418 --reason "upstream data corrected"
      dispatchio retry create --run-id 20260418 --dry-run
    """
    orch_path = _resolve_orchestrator_path(orchestrator)
    if not orch_path:
        _exit_with_error(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = _load_orchestrator(orch_path)

    if jobs:
        target_jobs = list(jobs)
    else:
        all_attempts = orch.state.list_attempts(logical_run_id=run_id)
        latest_by_job: dict[str, AttemptRecord] = {}
        for rec in all_attempts:
            if rec.job_name not in latest_by_job or rec.attempt > latest_by_job[rec.job_name].attempt:
                latest_by_job[rec.job_name] = rec
        target_jobs = [
            name
            for name, rec in latest_by_job.items()
            if rec.status in (Status.ERROR, Status.LOST)
        ]

    if not target_jobs:
        typer.echo("No failed jobs found for this run.")
        return

    if dry_run:
        typer.echo(f"[dry-run] Would retry {len(target_jobs)} job(s) for run {run_id}:")
        for job_name in sorted(target_jobs):
            latest = orch.state.get_latest_attempt(job_name, run_id)
            next_attempt = (latest.attempt + 1) if latest else 0
            typer.echo(f"  {job_name}  attempt={next_attempt}")
        return

    typer.echo(f"Retrying {len(target_jobs)} job(s) for run {run_id}:")
    for job_name in sorted(target_jobs):
        try:
            new_rec = orch.manual_retry(
                job_name,
                run_id,
                operator_name=operator,
                operator_reason=reason or "",
            )
            typer.echo(typer.style(f"  ↺ {job_name}  attempt={new_rec.attempt}", fg="cyan"))
        except ValueError as e:
            typer.echo(typer.style(f"  ✗ {job_name}  {e}", fg="red"))


@retry_app.command("list")
def retry_list(
    *,
    context_name: ContextOption = None,
    run_id: Annotated[
        Optional[str], typer.Option("--run-id", "-r", help="Filter by logical run ID.")
    ] = None,
    job: JobOption = None,
    attempt: Annotated[
        Optional[int], typer.Option("--attempt", "-a", help="Filter by attempt number.")
    ] = None,
    view: Annotated[
        RetryListView,
        typer.Option(
            "--filter",
            help="'attempts' shows execution history; 'requests' shows manual retry audit log.",
            show_default=True,
            case_sensitive=False,
        ),
    ] = RetryListView.ATTEMPTS,
    limit: Annotated[
        int, typer.Option("--limit", "-n", help="Max rows to show.", show_default=True)
    ] = 50,
) -> None:
    """List attempt history or manual retry requests.

    \b
    Examples:
      dispatchio retry list
      dispatchio retry list --run-id 20260418
      dispatchio retry list --filter requests
    """
    store = _load_store_from_context(context_name)
    if store is None:
        _exit_with_error(
            "Cannot locate state store. Use --context or run from a directory with dispatchio.toml."
        )

    if view == RetryListView.REQUESTS:
        requests = store.list_retry_requests(logical_run_id=run_id)
        if job:
            requests = [request for request in requests if job in request.selected_jobs]
        requests = requests[:limit]
        if not requests:
            typer.echo("No retry requests found.")
            return
        header = f"{'REQUESTED_AT':<27} {'BY':<16} {'RUN_ID':<14} {'JOBS'}"
        typer.echo(header)
        typer.echo("─" * len(header))
        for request in requests:
            jobs_str = ", ".join(request.selected_jobs)
            typer.echo(
                f"  {request.requested_at.isoformat():<25} {request.requested_by:<16} "
                f"{request.logical_run_id:<14} {jobs_str}"
            )
    else:
        records = store.list_attempts(
            job_name=job,
            logical_run_id=run_id,
            attempt=attempt,
        )
        records = records[:limit]
        if not records:
            typer.echo("No attempts found.")
            return
        _print_records(records)


@context_app.command("add")
def context_add(
    name: Annotated[str, typer.Argument(help="Context name.")],
    config_path: Annotated[str, typer.Argument(help="Path to config file.")],
    *,
    description: Annotated[
        str, typer.Option("--description", "-d", help="Optional description.")
    ] = "",
) -> None:
    """Register a named context pointing to CONFIG_PATH."""
    from dispatchio.contexts import ContextEntry, ContextStore

    abs_path = str(Path(config_path).expanduser().resolve())
    if not Path(abs_path).exists():
        _exit_with_error(f"Config file not found: {abs_path}")

    ContextStore().add(
        ContextEntry(name=name, config_path=abs_path, description=description)
    )
    typer.echo(f"Added context {name!r} → {abs_path}")


@context_app.command("list")
def context_list() -> None:
    """List all registered contexts."""
    from dispatchio.contexts import ContextStore

    store = ContextStore()
    entries = store.list()
    current = store.current_name()

    if not entries:
        typer.echo("No contexts registered. Use 'dispatchio context add' to register one.")
        return

    header = f"{'':2} {'NAME':<24} {'CONFIG PATH'}"
    typer.echo(header)
    typer.echo("─" * 70)
    for entry in entries:
        marker = "* " if entry.name == current else "  "
        desc = f"  ({entry.description})" if entry.description else ""
        typer.echo(f"{marker}{entry.name:<24} {entry.config_path}{desc}")


@context_app.command("use")
def context_use(name: Annotated[str, typer.Argument(help="Context name.")]) -> None:
    """Set NAME as the active context for subsequent commands."""
    from dispatchio.contexts import ContextStore

    try:
        ContextStore().use(name)
    except KeyError as e:
        _exit_with_error(str(e))
    typer.echo(f"Switched to context {name!r}.")


@context_app.command("remove")
def context_remove(name: Annotated[str, typer.Argument(help="Context name.")]) -> None:
    """Remove a registered context (does not delete any files)."""
    from dispatchio.contexts import ContextStore

    ContextStore().remove(name)
    typer.echo(f"Removed context {name!r}.")


@graph_app.command("validate")
def graph_validate(path: GraphPathArgument) -> None:
    """Validate a graph artifact file (Pydantic + graph-level checks).

    Reports all errors at once so producers can fix everything in one pass.

    \b
    Example:
      dispatchio graph validate graph.json
    """
    from dispatchio.graph import GraphValidationError, load_graph, validate_graph

    try:
        spec = load_graph(path)
    except GraphValidationError as exc:
        _exit_with_error(str(exc))

    try:
        validate_graph(spec)
    except GraphValidationError as exc:
        _exit_with_error(str(exc))

    typer.echo(typer.style(f"Graph {path} is valid.", fg="green"))
    typer.echo(f"  orchestrator : {spec.orchestrator_name}")
    typer.echo(f"  graph_version: {spec.graph_version}")
    typer.echo(f"  jobs         : {len(spec.jobs)}")
    if spec.external_dependencies:
        typer.echo(f"  external deps: {len(spec.external_dependencies)}")
    if spec.producer:
        typer.echo(f"  producer     : {spec.producer.name} {spec.producer.version}")


@graph_app.command("schema")
def graph_schema(
    *,
    output: Annotated[
        Optional[str],
        typer.Option("--output", "-o", help="Write schema to FILE instead of stdout.", metavar="FILE"),
    ] = None,
) -> None:
    """Print the JSON Schema for GraphSpec.

    Non-Python producers can use this schema to validate artifacts before
    publishing them.

    \b
    Examples:
      dispatchio graph schema
      dispatchio graph schema --output graph-spec-v1.json
    """
    import json as _json
    from dispatchio.graph import dump_schema

    schema_str = _json.dumps(dump_schema(), indent=2)
    if output:
        Path(output).write_text(schema_str)
        typer.echo(f"Schema written to {output}")
    else:
        typer.echo(schema_str)


@record_app.callback()
def record_callback() -> None:
    """Manually inspect or override run records."""


@retry_app.callback()
def retry_callback() -> None:
    """Create and list manual retry requests."""


@context_app.callback()
def context_callback() -> None:
    """Manage named orchestrator contexts (config file pointers)."""


@graph_app.callback()
def graph_callback() -> None:
    """Graph artifact utilities (JSON graph mode)."""


if __name__ == "__main__":
    app()

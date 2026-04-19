"""
Dispatchio CLI

Connects to a remote (or local) Dispatchio instance by importing the
orchestrator from a user-specified Python module path.

Configuration (in priority order):
  1. CLI flags  --orchestrator / --state-dir / --context
  2. Environment variables  DISPATCHIO_ORCHESTRATOR  DISPATCHIO_STATE_DIR
  3. dispatchio.toml  [dispatchio] orchestrator = "mymodule:orchestrator"

Usage:
  dispatchio tick [--reference-time "2025-01-15T02:00"]
  dispatchio status [--job JOB] [--run-id RUN_ID] [--status STATUS]
  dispatchio ticks [--limit N] [--since ISO] [--until ISO] [--detail]
  dispatchio record set JOB RUN_ID STATUS [--reason TEXT]
  dispatchio context add NAME CONFIG_PATH [--description TEXT]
  dispatchio context list
  dispatchio context use NAME
  dispatchio context remove NAME
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import importlib.util

import click

from dispatchio.models import AttemptRecord, Status, TickResult
from dispatchio.orchestrator import Orchestrator
from dispatchio.state import SQLAlchemyStateStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------


def _resolve_orchestrator_path(orch_path: str | None) -> str | None:
    return orch_path or os.environ.get("DISPATCHIO_ORCHESTRATOR")


def _load_orchestrator(path: str) -> Orchestrator:
    """
    Load an Orchestrator from a Python dotted path.
    Format: "module.path:attribute"  e.g. "myproject.jobs:orchestrator"
    """
    if ":" not in path:
        raise click.ClickException(
            f"Orchestrator path must be 'module:attribute', got: {path!r}"
        )
    module_path, attr = path.rsplit(":", 1)
    sys.path.insert(0, os.getcwd())
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        raise click.ClickException(f"Cannot import module {module_path!r}: {e}")
    obj = getattr(module, attr, None)
    if obj is None:
        raise click.ClickException(
            f"Attribute {attr!r} not found in module {module_path!r}"
        )
    if not isinstance(obj, Orchestrator):
        raise click.ClickException(
            f"{path!r} is a {type(obj).__name__}, expected Orchestrator"
        )
    return obj


def _load_store_from_context(context_name: str | None) -> SQLAlchemyStateStore | None:
    """
    Load a StateStore from a named (or active) context, or auto-discovered config.
    Returns None if no configuration can be located.
    """
    from dispatchio.contexts import ContextStore
    from dispatchio.config.loader import load_config, _build_state

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


def _resolve_tick_log_store(context_name: str | None):
    """
    Resolve the tick log store from a named (or active) context,
    or auto-discovered config. Returns None if no config can be located.
    """
    from dispatchio.tick_log import FilesystemTickLogStore
    from dispatchio.contexts import ContextStore
    from dispatchio.config.loader import load_config

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


# ---------------------------------------------------------------------------
# Shared options
# ---------------------------------------------------------------------------


def _orch_option(f):
    return click.option(
        "--orchestrator",
        "-o",
        default=None,
        help="module:attribute path to an Orchestrator, e.g. myproject.jobs:orchestrator",
    )(f)


def _context_option(f):
    return click.option(
        "--context",
        "-c",
        "context_name",
        default=None,
        help="Named context from ~/.dispatchio/contexts.json. "
        "Run 'dispatchio context list' to see available contexts.",
    )(f)


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------


@click.group()
def cli():
    """Dispatchio — lightweight tick-based batch job orchestrator."""


# ---------------------------------------------------------------------------
# tick
# ---------------------------------------------------------------------------


@cli.command()
@_orch_option
@click.option(
    "--reference-time",
    "-t",
    default=None,
    help="ISO-8601 datetime to use as the tick reference time. Defaults to now (UTC).",
)
def tick(orchestrator: str | None, reference_time: str | None):
    """Run one orchestrator tick and print results."""
    orch_path = _resolve_orchestrator_path(orchestrator)
    if not orch_path:
        raise click.ClickException(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = _load_orchestrator(orch_path)

    ref = None
    if reference_time:
        ref = datetime.fromisoformat(reference_time)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)

    result: TickResult = orch.tick(reference_time=ref)

    click.echo(f"\nTick at {result.reference_time.isoformat()}")
    click.echo(f"{'─' * 60}")
    if not result.results:
        click.echo("  (no actions taken)")
    else:
        for r in result.results:
            marker = _action_icon(r.action.value)
            detail = f"  {r.detail}" if r.detail else ""
            click.echo(
                f"  {marker} {r.job_name}[{r.run_id}] → {r.action.value}{detail}"
            )
    click.echo()


# ---------------------------------------------------------------------------
# run  (PythonJob entry point)
# ---------------------------------------------------------------------------


@cli.command("run")
@click.argument("entry_point", required=False, default=None)
@click.option("--script", default=None, help="Path to a Python script file.")
@click.option(
    "--function",
    "function_name",
    default=None,
    help="Function name within the script (required with --script).",
)
@click.option(
    "--job-name", default=None, help="Job name override. Defaults to the function name."
)
def run_command(
    entry_point: str | None,
    script: str | None,
    function_name: str | None,
    job_name: str | None,
):
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
            raise click.ClickException(
                f"entry_point must be 'module:function', got: {entry_point!r}"
            )
        module_path, func_name = entry_point.rsplit(":", 1)
        sys.path.insert(0, os.getcwd())
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError as e:
            raise click.ClickException(f"Cannot import module {module_path!r}: {e}")
        fn = getattr(module, func_name, None)
        if fn is None or not callable(fn):
            raise click.ClickException(
                f"Callable {func_name!r} not found in module {module_path!r}"
            )

    elif script is not None:
        if function_name is None:
            raise click.ClickException("--function is required when --script is used.")
        script_path = Path(script).resolve()
        if not script_path.exists():
            raise click.ClickException(f"Script not found: {script_path}")
        spec = importlib.util.spec_from_file_location("_dispatchio_job", script_path)
        module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(module)  # type: ignore[union-attr]
        fn = getattr(module, function_name, None)
        func_name = function_name
        if fn is None or not callable(fn):
            raise click.ClickException(
                f"Function {function_name!r} not found in {script_path}"
            )

    else:
        raise click.ClickException(
            "Provide either ENTRY_POINT (module:function) "
            "or both --script and --function."
        )

    resolved_job_name = job_name or func_name
    run_job(resolved_job_name, fn)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@cli.command()
@_context_option
@click.option("--job", "-j", default=None, help="Filter by job name.")
@click.option("--run-id", "-r", default=None, help="Filter by run_id.")
@click.option(
    "--status",
    "filter_status",
    default=None,
    type=click.Choice([s.value for s in Status]),
    help="Filter by status.",
)
def status(
    context_name: str | None,
    job: str | None,
    run_id: str | None,
    filter_status: str | None,
):
    """Show the status of job runs."""
    store = _load_store_from_context(context_name)
    if store is None:
        raise click.ClickException(
            "Cannot locate state store. Use --context or run from a directory "
            "with dispatchio.toml."
        )
    status_filter = Status(filter_status) if filter_status else None
    records = store.list_attempts(job_name=job, status=status_filter)

    if run_id:
        records = [r for r in records if r.logical_run_id == run_id]

    if not records:
        click.echo("No records found.")
        return

    _print_records(records)


# ---------------------------------------------------------------------------
# ticks
# ---------------------------------------------------------------------------


@cli.command()
@_context_option
@click.option("--limit", "-n", default=20, show_default=True, help="Max ticks to show.")
@click.option(
    "--since", default=None, help="Show ticks at or after this ISO timestamp."
)
@click.option(
    "--until", default=None, help="Show ticks at or before this ISO timestamp."
)
@click.option(
    "--detail", is_flag=True, default=False, help="Show individual job actions."
)
def ticks(
    context_name: str | None,
    limit: int,
    since: str | None,
    until: str | None,
    detail: bool,
):
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
        raise click.ClickException(
            "Cannot locate tick log. Specify --state-dir, --context, "
            "or run from a directory with dispatchio.toml."
        )

    records = store.list(limit=limit, since=since, until=until)

    if not records:
        click.echo("No tick records found.")
        return

    for rec in records:
        submitted = sum(1 for a in rec.actions if a["action"] == "submitted")
        total = len(rec.actions)
        summary = f"{total} action(s)"
        if submitted:
            summary += f"  ({submitted} submitted)"

        click.echo(
            f"  {rec.ticked_at}  ref={rec.reference_time[:10]}  "
            f"{rec.duration_seconds:.2f}s  {summary}"
        )

        if detail:
            for action in rec.actions:
                marker = _action_icon(action["action"])
                d = f"  {action['detail']}" if action.get("detail") else ""
                click.echo(
                    f"    {marker} {action['job_name']}[{action['run_id']}]"
                    f" → {action['action']}{d}"
                )


# ---------------------------------------------------------------------------
# record set  (manual override)
# ---------------------------------------------------------------------------


@cli.group("record")
def record_group():
    """Manually inspect or override run records."""


@record_group.command("set")
@_context_option
@click.argument("job_name")
@click.argument("run_id")
@click.argument(
    "new_status", metavar="STATUS", type=click.Choice([s.value for s in Status])
)
@click.option("--reason", default=None, help="Error reason string.")
def record_set(
    context_name: str | None,
    job_name: str,
    run_id: str,
    new_status: str,
    reason: str | None,
):
    """Manually set the status of a run record."""
    store = _load_store_from_context(context_name)
    if store is None:
        raise click.ClickException(
            "Cannot locate state store. Use --context or run from a directory "
            "with dispatchio.toml."
        )
    existing = store.get_latest_attempt(job_name, run_id)
    if existing is None:
        record = AttemptRecord(
            job_name=job_name,
            logical_run_id=run_id,
            attempt=0,
            dispatchio_attempt_id=uuid4(),
            status=Status(new_status),
            reason=reason,
        )
        store.append_attempt(record)
    else:
        record = existing.model_copy(
            update={"status": Status(new_status), "reason": reason}
        )
        store.update_attempt(record)
    click.echo(f"Set {job_name}[{run_id}] → {new_status}")


# ---------------------------------------------------------------------------
# retry
# ---------------------------------------------------------------------------


@cli.group("retry")
def retry_group():
    """Create and list manual retry requests."""


@retry_group.command("create")
@_orch_option
@_context_option
@click.option("--run-id", "-r", required=True, help="Logical run ID to retry.")
@click.option(
    "--jobs",
    "-j",
    multiple=True,
    help="Jobs to retry. If omitted, retries all failed jobs for the run.",
)
@click.option("--reason", default=None, help="Reason for this retry.")
@click.option(
    "--operator",
    default="cli",
    show_default=True,
    help="Name/ID of the operator requesting the retry.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print selected jobs and assigned attempt numbers without writing state.",
)
def retry_create(
    orchestrator: str | None,
    context_name: str | None,
    run_id: str,
    jobs: tuple[str, ...],
    reason: str | None,
    operator: str,
    dry_run: bool,
):
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
        raise click.ClickException(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = _load_orchestrator(orch_path)

    # Resolve target jobs: explicit list or all terminal-failed for this run
    if jobs:
        target_jobs = list(jobs)
    else:
        all_attempts = orch.state.list_attempts(logical_run_id=run_id)
        latest_by_job: dict[str, AttemptRecord] = {}
        for rec in all_attempts:
            if (
                rec.job_name not in latest_by_job
                or rec.attempt > latest_by_job[rec.job_name].attempt
            ):
                latest_by_job[rec.job_name] = rec
        target_jobs = [
            name
            for name, rec in latest_by_job.items()
            if rec.status in (Status.ERROR, Status.LOST)
        ]

    if not target_jobs:
        click.echo("No failed jobs found for this run.")
        return

    if dry_run:
        click.echo(f"[dry-run] Would retry {len(target_jobs)} job(s) for run {run_id}:")
        for job_name in sorted(target_jobs):
            latest = orch.state.get_latest_attempt(job_name, run_id)
            next_attempt = (latest.attempt + 1) if latest else 0
            click.echo(f"  {job_name}  attempt={next_attempt}")
        return

    click.echo(f"Retrying {len(target_jobs)} job(s) for run {run_id}:")
    for job_name in sorted(target_jobs):
        try:
            new_rec = orch.manual_retry(
                job_name, run_id, operator_name=operator, operator_reason=reason or ""
            )
            click.echo(
                click.style(f"  ↺ {job_name}  attempt={new_rec.attempt}", fg="cyan")
            )
        except ValueError as e:
            click.echo(click.style(f"  ✗ {job_name}  {e}", fg="red"))


@retry_group.command("list")
@_context_option
@click.option("--run-id", "-r", default=None, help="Filter by logical run ID.")
@click.option("--job", "-j", default=None, help="Filter by job name.")
@click.option(
    "--attempt", "-a", default=None, type=int, help="Filter by attempt number."
)
@click.option(
    "--filter",
    "view",
    default="attempts",
    type=click.Choice(["attempts", "requests"]),
    show_default=True,
    help="'attempts' shows execution history; 'requests' shows manual retry audit log.",
)
@click.option("--limit", "-n", default=50, show_default=True, help="Max rows to show.")
def retry_list(
    context_name: str | None,
    run_id: str | None,
    job: str | None,
    attempt: int | None,
    view: str,
    limit: int,
):
    """List attempt history or manual retry requests.

    \b
    Examples:
      dispatchio retry list
      dispatchio retry list --run-id 20260418
      dispatchio retry list --filter requests
    """
    store = _load_store_from_context(context_name)
    if store is None:
        raise click.ClickException(
            "Cannot locate state store. Use --context or run from a directory "
            "with dispatchio.toml."
        )

    if view == "requests":
        requests = store.list_retry_requests(logical_run_id=run_id)
        if job:
            requests = [r for r in requests if job in r.selected_jobs]
        requests = requests[:limit]
        if not requests:
            click.echo("No retry requests found.")
            return
        header = f"{'REQUESTED_AT':<27} {'BY':<16} {'RUN_ID':<14} {'JOBS'}"
        click.echo(header)
        click.echo("─" * len(header))
        for r in requests:
            jobs_str = ", ".join(r.selected_jobs)
            click.echo(
                f"  {r.requested_at.isoformat():<25} {r.requested_by:<16} "
                f"{r.logical_run_id:<14} {jobs_str}"
            )
    else:
        records = store.list_attempts(
            job_name=job,
            logical_run_id=run_id,
            attempt=attempt,
        )
        records = records[:limit]
        if not records:
            click.echo("No attempts found.")
            return
        _print_records(records)


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------


@cli.command("cancel")
@_context_option
@click.option("--run-id", "-r", required=True, help="Logical run ID.")
@click.option("--job", "-j", required=True, help="Job name.")
@click.option(
    "--attempt",
    "-a",
    default=None,
    type=int,
    help="Attempt number. Defaults to latest active attempt.",
)
@click.option("--reason", default=None, help="Reason for cancellation.")
@click.option(
    "--operator",
    default="cli",
    show_default=True,
    help="Name/ID of the operator requesting cancellation.",
)
def cancel(
    context_name: str | None,
    run_id: str,
    job: str,
    attempt: int | None,
    reason: str | None,
    operator: str,
):
    """Cancel an active attempt.

    \b
    Examples:
      dispatchio cancel --run-id 20260418 --job load_sales
      dispatchio cancel --run-id 20260418 --job load_sales --attempt 1 --reason "hung"
    """
    store = _load_store_from_context(context_name)
    if store is None:
        raise click.ClickException(
            "Cannot locate state store. Use --context or run from a directory "
            "with dispatchio.toml."
        )

    if attempt is not None:
        candidates = store.list_attempts(
            job_name=job, logical_run_id=run_id, attempt=attempt
        )
        rec = candidates[0] if candidates else None
    else:
        rec = store.get_latest_attempt(job, run_id)
        if rec is not None and rec.is_finished():
            raise click.ClickException(
                f"{job}[{run_id}] latest attempt {rec.attempt} is already {rec.status.value} (terminal). "
                "Specify --attempt to cancel a specific attempt."
            )

    if rec is None:
        raise click.ClickException(f"No attempt found for {job}[{run_id}].")

    if rec.is_finished():
        raise click.ClickException(
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
    click.echo(
        click.style(
            f"Cancelled {job}[{run_id}] attempt {rec.attempt}.",
            fg="yellow",
        )
    )


# ---------------------------------------------------------------------------
# context
# ---------------------------------------------------------------------------


@cli.group("context")
def context_group():
    """Manage named orchestrator contexts (config file pointers).

    Contexts let you switch between multiple orchestrators without retyping
    config paths. They are stored at ~/.dispatchio/contexts.json.

    \b
    Quick start:
      dispatchio context add daily-etl /srv/pipelines/daily/dispatchio.toml
      dispatchio context add weekly-reports /srv/pipelines/weekly/dispatchio.toml
      dispatchio context use daily-etl
      dispatchio ticks            # inspects daily-etl
      dispatchio ticks --context weekly-reports
    """


@context_group.command("add")
@click.argument("name")
@click.argument("config_path")
@click.option("--description", "-d", default="", help="Optional description.")
def context_add(name: str, config_path: str, description: str):
    """Register a named context pointing to CONFIG_PATH."""
    from dispatchio.contexts import ContextEntry, ContextStore

    abs_path = str(Path(config_path).expanduser().resolve())
    if not Path(abs_path).exists():
        raise click.ClickException(f"Config file not found: {abs_path}")

    ContextStore().add(
        ContextEntry(name=name, config_path=abs_path, description=description)
    )
    click.echo(f"Added context {name!r} → {abs_path}")


@context_group.command("list")
def context_list():
    """List all registered contexts."""
    from dispatchio.contexts import ContextStore

    store = ContextStore()
    entries = store.list()
    current = store.current_name()

    if not entries:
        click.echo(
            "No contexts registered. Use 'dispatchio context add' to register one."
        )
        return

    header = f"{'':2} {'NAME':<24} {'CONFIG PATH'}"
    click.echo(header)
    click.echo("─" * 70)
    for e in entries:
        marker = "* " if e.name == current else "  "
        desc = f"  ({e.description})" if e.description else ""
        click.echo(f"{marker}{e.name:<24} {e.config_path}{desc}")


@context_group.command("use")
@click.argument("name")
def context_use(name: str):
    """Set NAME as the active context for subsequent commands."""
    from dispatchio.contexts import ContextStore

    try:
        ContextStore().use(name)
    except KeyError as e:
        raise click.ClickException(str(e))
    click.echo(f"Switched to context {name!r}.")


@context_group.command("remove")
@click.argument("name")
def context_remove(name: str):
    """Remove a registered context (does not delete any files)."""
    from dispatchio.contexts import ContextStore

    ContextStore().remove(name)
    click.echo(f"Removed context {name!r}.")


# ---------------------------------------------------------------------------
# graph
# ---------------------------------------------------------------------------


@cli.group("graph")
def graph_group():
    """Graph artifact utilities (JSON graph mode)."""


@graph_group.command("validate")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
def graph_validate(path: Path):
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
        raise click.ClickException(str(exc))

    try:
        validate_graph(spec)
    except GraphValidationError as exc:
        raise click.ClickException(str(exc))

    click.echo(click.style(f"Graph {path} is valid.", fg="green"))
    click.echo(f"  orchestrator : {spec.orchestrator_name}")
    click.echo(f"  graph_version: {spec.graph_version}")
    click.echo(f"  jobs         : {len(spec.jobs)}")
    if spec.external_dependencies:
        click.echo(f"  external deps: {len(spec.external_dependencies)}")
    if spec.producer:
        click.echo(f"  producer     : {spec.producer.name} {spec.producer.version}")


@graph_group.command("schema")
@click.option(
    "--output",
    "-o",
    default=None,
    help="Write schema to FILE instead of stdout.",
    metavar="FILE",
)
def graph_schema(output: str | None):
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
        click.echo(f"Schema written to {output}")
    else:
        click.echo(schema_str)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

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
    click.echo(header)
    click.echo("─" * len(header))
    for r in records:
        colour, label = _STATUS_COLOURS.get(r.status.value, ("white", r.status.value))
        completed = r.completed_at.isoformat() if r.completed_at else "—"
        line = f"{r.job_name:<30} {r.logical_run_id:<14} {label:<10} {r.attempt:<8} {completed}"
        click.echo(click.style(line, fg=colour))

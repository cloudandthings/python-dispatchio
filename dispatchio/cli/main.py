"""
Dispatchio CLI

Connects to a remote (or local) Dispatchio instance by importing the
orchestrator from a user-specified Python module path.

Configuration (in priority order):
  1. CLI flags  --orchestrator / --state-dir
  2. Environment variables  DISPATCHIO_ORCHESTRATOR  DISPATCHIO_STATE_DIR
  3. dispatchio.toml  [dispatchio] orchestrator = "mymodule:orchestrator"

Usage:
  dispatchio tick [--reference-time "2025-01-15T02:00"]
  dispatchio status [--job JOB] [--run-id RUN_ID] [--status STATUS]
  dispatchio record set JOB RUN_ID STATUS [--reason TEXT]
  dispatchio heartbeat JOB RUN_ID
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import importlib.util

import click
from pydantic import BaseModel

from dispatchio.models import RunRecord, Status, TickResult
from dispatchio.orchestrator import Orchestrator
from dispatchio.state.filesystem import FilesystemStateStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------

def _resolve_state_dir(state_dir: str | None) -> str | None:
    return state_dir or os.environ.get("DISPATCHIO_STATE_DIR")


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


def _load_state(state_dir: str) -> FilesystemStateStore:
    return FilesystemStateStore(state_dir)


# ---------------------------------------------------------------------------
# Shared options
# ---------------------------------------------------------------------------

def _orch_option(f):
    return click.option(
        "--orchestrator", "-o",
        default=None,
        help="module:attribute path to an Orchestrator, e.g. myproject.jobs:orchestrator",
    )(f)


def _state_option(f):
    return click.option(
        "--state-dir", "-s",
        default=None,
        help="Path to state directory (FilesystemStateStore root). "
             "Env: DISPATCHIO_STATE_DIR",
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
    "--reference-time", "-t",
    default=None,
    help="ISO-8601 datetime to use as the tick reference time. "
         "Defaults to now (UTC).",
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
            click.echo(f"  {marker} {r.job_name}[{r.run_id}] → {r.action.value}{detail}")
    click.echo()


# ---------------------------------------------------------------------------
# run  (PythonJob entry point)
# ---------------------------------------------------------------------------

@cli.command("run")
@click.argument("entry_point", required=False, default=None)
@click.option("--script",   default=None, help="Path to a Python script file.")
@click.option("--function", "function_name", default=None,
              help="Function name within the script (required with --script).")
@click.option("--job-name", default=None,
              help="Job name override. Defaults to the function name.")
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
@_state_option
@click.option("--job", "-j", default=None, help="Filter by job name.")
@click.option("--run-id", "-r", default=None, help="Filter by run_id.")
@click.option(
    "--status", "filter_status",
    default=None,
    type=click.Choice([s.value for s in Status]),
    help="Filter by status.",
)
def status(
    state_dir: str | None,
    job: str | None,
    run_id: str | None,
    filter_status: str | None,
):
    """Show the status of job runs."""
    sd = _resolve_state_dir(state_dir)
    if not sd:
        raise click.ClickException(
            "Specify a state directory with --state-dir or DISPATCHIO_STATE_DIR"
        )
    store = _load_state(sd)
    status_filter = Status(filter_status) if filter_status else None
    records = store.list_records(job_name=job, status=status_filter)

    if run_id:
        records = [r for r in records if r.run_id == run_id]

    if not records:
        click.echo("No records found.")
        return

    _print_records(records)


# ---------------------------------------------------------------------------
# record set  (manual override)
# ---------------------------------------------------------------------------

@cli.group("record")
def record_group():
    """Manually inspect or override run records."""


@record_group.command("set")
@_state_option
@click.argument("job_name")
@click.argument("run_id")
@click.argument("new_status", metavar="STATUS",
                type=click.Choice([s.value for s in Status]))
@click.option("--reason", default=None, help="Error reason string.")
def record_set(
    state_dir: str | None,
    job_name: str,
    run_id: str,
    new_status: str,
    reason: str | None,
):
    """Manually set the status of a run record."""
    sd = _resolve_state_dir(state_dir)
    if not sd:
        raise click.ClickException("Specify --state-dir or DISPATCHIO_STATE_DIR")
    store = _load_state(sd)
    existing = store.get(job_name, run_id)
    if existing is None:
        record = RunRecord(
            job_name=job_name,
            run_id=run_id,
            status=Status(new_status),
            error_reason=reason,
        )
    else:
        record = existing.model_copy(
            update={"status": Status(new_status), "error_reason": reason}
        )
    store.put(record)
    click.echo(f"Set {job_name}[{run_id}] → {new_status}")


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------

@cli.command()
@_state_option
@click.argument("job_name")
@click.argument("run_id")
def heartbeat(state_dir: str | None, job_name: str, run_id: str):
    """Send a manual heartbeat for a running job."""
    sd = _resolve_state_dir(state_dir)
    if not sd:
        raise click.ClickException("Specify --state-dir or DISPATCHIO_STATE_DIR")
    store = _load_state(sd)
    store.heartbeat(job_name, run_id)
    click.echo(f"Heartbeat recorded for {job_name}[{run_id}]")


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_STATUS_COLOURS = {
    "done":      ("green",  "DONE     "),
    "submitted": ("cyan",   "SUBMITTED"),
    "running":   ("blue",   "RUNNING  "),
    "error":     ("red",    "ERROR    "),
    "lost":      ("yellow", "LOST     "),
    "pending":   ("white",  "PENDING  "),
    "skipped":   ("white",  "SKIPPED  "),
}

_ACTION_ICONS = {
    "submitted":              "✓",
    "retrying":               "↺",
    "marked_lost":            "✗",
    "marked_error":           "✗",
    "submission_failed":      "✗",
    "skipped_condition":      "·",
    "skipped_dependencies":   "·",
    "skipped_already_active": "·",
    "skipped_already_done":   "·",
}


def _action_icon(action: str) -> str:
    return _ACTION_ICONS.get(action, "?")


def _print_records(records: list[RunRecord]) -> None:
    header = f"{'JOB':<30} {'RUN_ID':<14} {'STATUS':<10} {'ATTEMPT':<8} {'COMPLETED'}"
    click.echo(header)
    click.echo("─" * len(header))
    for r in records:
        colour, label = _STATUS_COLOURS.get(r.status.value, ("white", r.status.value))
        completed = r.completed_at.isoformat() if r.completed_at else "—"
        line = f"{r.job_name:<30} {r.run_id:<14} {label:<10} {r.attempt:<8} {completed}"
        click.echo(click.style(line, fg=colour))

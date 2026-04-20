from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from datetime import datetime, timezone
from typing import Annotated

import typer

from dispatchio.cli import output
from dispatchio.cli.app import app
from dispatchio.cli.errors import handle_cli_errors
from dispatchio.cli.errors import CliUserError
from dispatchio.cli.loaders import (
    load_orchestrator,
    load_store_from_context,
    resolve_tick_log_store,
)
from dispatchio.cli.options import (
    AttemptOption,
    ContextOption,
    EntryPointArgument,
    FunctionNameArgument,
    JobOption,
    JobRequiredOption,
    LimitOption,
    OperatorOption,
    OrchestratorOption,
    ReasonOption,
    ReferenceTimeOption,
    RunIdOption,
    RunIdRequiredOption,
    ScriptPathArgument,
    StatusFilterOption,
    YesOption,
)
from dispatchio.models import Status, TickResult


@app.command()
@handle_cli_errors
def tick(
    *,
    orchestrator: OrchestratorOption = None,
    reference_time: ReferenceTimeOption = None,
) -> None:
    """Run one orchestrator tick and print results."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )

    with output.console.status("Loading orchestrator..."):
        orch = load_orchestrator(orchestrator)

    ref = None
    if reference_time:
        ref = datetime.fromisoformat(reference_time)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)

    result: TickResult = orch.tick(reference_time=ref)
    output.print_tick_result(result)


@app.command("run")
@handle_cli_errors
def run_command(
    entry_point: EntryPointArgument,
    *,
    job_name: Annotated[
        str | None,
        typer.Option(
            "--job-name",
            help="Job name override. Defaults to the function name.",
            show_default=False,
        ),
    ] = None,
) -> None:
    """Run a Python callable as a Dispatchio job from MODULE:FUNCTION."""
    from dispatchio.worker.harness import run_job

    if ":" not in entry_point:
        raise CliUserError(
            f"entry_point must be 'module:function', got: {entry_point!r}"
        )

    module_path, func_name = entry_point.rsplit(":", 1)
    sys.path.insert(0, os.getcwd())
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        raise CliUserError(f"Cannot import module {module_path!r}: {exc}") from exc

    fn = getattr(module, func_name, None)
    if fn is None or not callable(fn):
        raise CliUserError(
            f"Callable {func_name!r} not found in module {module_path!r}"
        )

    run_job(job_name or func_name, fn)


@app.command("run-script")
@handle_cli_errors
def run_script(
    script_path: ScriptPathArgument,
    function_name: FunctionNameArgument,
    *,
    job_name: Annotated[
        str | None,
        typer.Option(
            "--job-name",
            help="Job name override. Defaults to the function name.",
            show_default=False,
        ),
    ] = None,
) -> None:
    """Run a Python callable as a Dispatchio job from FILE FUNCTION."""
    from dispatchio.worker.harness import run_job

    spec = importlib.util.spec_from_file_location("_dispatchio_job", script_path)
    if spec is None or spec.loader is None:
        raise CliUserError(f"Unable to load script: {script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fn = getattr(module, function_name, None)
    if fn is None or not callable(fn):
        raise CliUserError(f"Function {function_name!r} not found in {script_path}")

    run_job(job_name or function_name, fn)


@app.command()
@handle_cli_errors
def status(
    *,
    context_name: ContextOption = None,
    job: JobOption = None,
    run_id: RunIdOption = None,
    filter_status: StatusFilterOption = None,
) -> None:
    """Show the status of job runs."""
    with output.console.status("Loading state..."):
        store = load_store_from_context(context_name)

    records = store.list_attempts(job_name=job, status=filter_status)
    if run_id:
        records = [record for record in records if record.logical_run_id == run_id]

    if not records:
        output.print_warning("No records found.")
        return

    output.print_records(records)


@app.command()
@handle_cli_errors
def ticks(
    *,
    context_name: ContextOption = None,
    limit: LimitOption = 20,
    since: Annotated[
        str | None,
        typer.Option("--since", help="Show ticks at or after this ISO timestamp."),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option("--until", help="Show ticks at or before this ISO timestamp."),
    ] = None,
    detail: Annotated[
        bool,
        typer.Option("--detail", help="Show individual job actions."),
    ] = False,
) -> None:
    """Show tick history for this orchestrator."""
    with output.console.status("Loading tick log..."):
        store = resolve_tick_log_store(context_name)

    records = store.list(limit=limit, since=since, until=until)
    if not records:
        output.print_warning("No tick records found.")
        return

    for record in records:
        output.print_tick_summary(record, detail=detail)


@handle_cli_errors
def cancel(
    *,
    context_name: ContextOption = None,
    run_id: RunIdRequiredOption,
    job: JobRequiredOption,
    attempt: AttemptOption = None,
    reason: ReasonOption = None,
    operator: OperatorOption = "cli",
    yes: YesOption = False,
) -> None:
    """Cancel an active attempt."""
    with output.console.status("Loading state..."):
        store = load_store_from_context(context_name)

    if attempt is not None:
        candidates = store.list_attempts(
            job_name=job, logical_run_id=run_id, attempt=attempt
        )
        rec = candidates[0] if candidates else None
    else:
        rec = store.get_latest_attempt(job, run_id)
        if rec is not None and rec.is_finished():
            raise CliUserError(
                f"{job}[{run_id}] latest attempt {rec.attempt} is already {rec.status.value} (terminal). "
                "Specify --attempt to cancel a specific attempt."
            )

    if rec is None:
        raise CliUserError(f"No attempt found for {job}[{run_id}].")

    if rec.is_finished():
        raise CliUserError(
            f"{job}[{run_id}] attempt {rec.attempt} is already {rec.status.value} (terminal)."
        )

    if not yes and not output.confirm(f"Cancel {job}[{run_id}] attempt {rec.attempt}?"):
        raise typer.Exit()

    cancelled = rec.model_copy(
        update={
            "status": Status.CANCELLED,
            "completed_at": datetime.now(tz=timezone.utc),
            "reason": reason,
            "operator_name": operator,
        }
    )
    store.update_attempt(cancelled)
    output.print_warning(f"Cancelled {job}[{run_id}] attempt {rec.attempt}.")

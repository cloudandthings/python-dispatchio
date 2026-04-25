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
    AllNamespacesOption,
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
    RunKeyOption,
    RunKeyRequiredOption,
    ScriptPathArgument,
    StatusFilterOption,
    YesOption,
)
from dispatchio.models import Status, TickResult
from dispatchio.models import OrchestratorRunMode, OrchestratorRunStatus


@app.command()
@handle_cli_errors
def tick(
    *,
    orchestrator: OrchestratorOption = None,
    reference_time: ReferenceTimeOption = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Plan only — show what would be submitted without touching state.",
        ),
    ] = False,
    pool: Annotated[
        str | None,
        typer.Option(
            "--pool",
            help="Only evaluate jobs in this pool.",
            show_default=False,
        ),
    ] = None,
) -> None:
    """Run one orchestrator tick and print results.

    With --dry-run, no completion events are drained, no lost-job detection
    runs, and nothing is submitted. Use it to preview what a real tick would do.
    """
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )

    with output.console.status("Loading orchestrator..."):
        orch = load_orchestrator(orchestrator)

    if pool is not None:
        declared_pools = sorted(orch.admission_policy.pools.keys())
        if pool not in orch.admission_policy.pools:
            raise CliUserError(
                f"Unknown pool {pool!r}. Declared pools: {declared_pools}"
            )

    ref = None
    if reference_time:
        ref = datetime.fromisoformat(reference_time)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)

    result: TickResult = orch.tick(reference_time=ref, dry_run=dry_run, pool=pool)
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


@app.command("run-file")
@handle_cli_errors
def run_file(
    script_path: ScriptPathArgument,
    *,
    run_key: Annotated[
        str | None,
        typer.Option(
            "--run-key",
            help=(
                "Explicit run key for all discovered jobs (uses LiteralCadence). "
                "Without this flag the jobs' declared cadences apply and a normal "
                "tick runs at the current wall-clock time."
            ),
            show_default=False,
        ),
    ] = None,
    ignore_dependencies: Annotated[
        bool,
        typer.Option(
            "--ignore-dependencies",
            help="Submit jobs regardless of whether their dependencies are satisfied.",
        ),
    ] = False,
) -> None:
    """Discover @job-decorated functions in FILE and run one orchestrator tick.

    Each function annotated with @dispatchio.job is registered as a job.
    The orchestrator is wired from dispatchio.toml (or DISPATCHIO_CONFIG).

    With --run-key, every job's cadence is overridden to LiteralCadence so
    the supplied key is used directly.  Dependency checks still apply unless
    --ignore-dependencies is also set.
    """
    from dispatchio.cadence import LiteralCadence
    from dispatchio.config import load_config, orchestrator as build_orchestrator
    from dispatchio.models import JobDependency

    spec = importlib.util.spec_from_file_location("_dispatchio_script", script_path)
    if spec is None or spec.loader is None:
        raise CliUserError(f"Unable to load script: {script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    jobs = [
        obj._dispatchio_job
        for obj in module.__dict__.values()
        if callable(obj) and hasattr(obj, "_dispatchio_job")
    ]

    if not jobs:
        raise CliUserError(
            f"No @job-decorated functions found in {script_path}. "
            "Annotate your functions with @dispatchio.job."
        )

    if run_key is not None:
        literal = LiteralCadence(value=run_key)
        updated = []
        for j in jobs:
            new_deps = [
                dep.model_copy(update={"cadence": literal})
                if isinstance(dep, JobDependency)
                else dep
                for dep in j.depends_on
            ]
            updated.append(
                j.model_copy(update={"cadence": literal, "depends_on": new_deps})
            )
        jobs = updated

    if ignore_dependencies:
        jobs = [j.model_copy(update={"depends_on": []}) for j in jobs]

    settings = load_config()
    orch = build_orchestrator(jobs, config=settings)
    result = orch.tick()
    output.print_tick_result(result)


@app.command()
@handle_cli_errors
def status(
    *,
    context_name: ContextOption = None,
    job: JobOption = None,
    run_key: RunKeyOption = None,
    filter_status: StatusFilterOption = None,
    all_namespaces: AllNamespacesOption = False,
) -> None:
    """Show the status of job runs."""
    with output.console.status("Loading state..."):
        store = load_store_from_context(context_name, all_namespaces=all_namespaces)

    records = store.list_attempts(job_name=job, status=filter_status)
    if run_key:
        records = [record for record in records if record.run_key == run_key]

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


@app.command()
@handle_cli_errors
def cancel(
    *,
    context_name: ContextOption = None,
    run_key: RunKeyRequiredOption,
    job: JobRequiredOption,
    attempt: AttemptOption = None,
    reason: ReasonOption = None,
    requested_by: OperatorOption = "cli",
    yes: YesOption = False,
) -> None:
    """Cancel an active attempt."""
    with output.console.status("Loading state..."):
        store = load_store_from_context(context_name)

    if attempt is not None:
        candidates = store.list_attempts(job_name=job, run_key=run_key, attempt=attempt)
        rec = candidates[0] if candidates else None
    else:
        all_recs = store.list_attempts(job_name=job, run_key=run_key)
        rec = max(all_recs, key=lambda r: r.attempt) if all_recs else None
        if rec is not None and rec.is_finished():
            raise CliUserError(
                f"{job}[{run_key}] latest attempt {rec.attempt} is already {rec.status.value} (terminal). "
                "Specify --attempt to cancel a specific attempt."
            )

    if rec is None:
        raise CliUserError(f"No attempt found for {job}[{run_key}].")

    if rec.is_finished():
        raise CliUserError(
            f"{job}[{run_key}] attempt {rec.attempt} is already {rec.status.value} (terminal)."
        )

    if not yes and not output.confirm(
        f"Cancel {job}[{run_key}] attempt {rec.attempt}?"
    ):
        raise typer.Exit()

    cancelled = rec.model_copy(
        update={
            "status": Status.CANCELLED,
            "completed_at": datetime.now(tz=timezone.utc),
            "reason": reason,
            "requested_by": requested_by,
        }
    )
    store.update_attempt(cancelled)
    output.print_warning(f"Cancelled {job}[{run_key}] attempt {rec.attempt}.")


def _parse_iso_datetime(value: str, *, option_name: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise CliUserError(f"Invalid {option_name}: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@app.command("run-list")
@handle_cli_errors
def run_list(
    *,
    orchestrator: OrchestratorOption = None,
    status: Annotated[
        OrchestratorRunStatus | None,
        typer.Option("--status", help="Filter by run status.", case_sensitive=False),
    ] = None,
    mode: Annotated[
        OrchestratorRunMode | None,
        typer.Option("--mode", help="Filter by run mode.", case_sensitive=False),
    ] = None,
) -> None:
    """List orchestrator runs."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = load_orchestrator(orchestrator)
    runs = orch.list_runs(status=status, mode=mode)
    if not runs:
        output.print_warning("No orchestrator runs found.")
        return
    for run in runs:
        output.print_info(
            f"{run.id}  {run.run_key}  status={run.status.value}  mode={run.mode.value}  priority={run.priority}"
        )


@app.command("run-show")
@handle_cli_errors
def run_show(
    *,
    orchestrator: OrchestratorOption = None,
    orchestrator_run_id: Annotated[
        int,
        typer.Option("--orchestrator-run-id", help="Run ID."),
    ],
) -> None:
    """Show one orchestrator run by ID."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = load_orchestrator(orchestrator)
    run = orch.show_run(orchestrator_run_id)
    output.print_info(f"id                 : {run.id}")
    output.print_info(f"namespace          : {run.namespace}")
    output.print_info(f"run_key            : {run.run_key}")
    output.print_info(f"status             : {run.status.value}")
    output.print_info(f"mode               : {run.mode.value}")
    output.print_info(f"priority           : {run.priority}")
    output.print_info(f"submitted_by       : {run.submitted_by}")
    output.print_info(f"reason             : {run.reason}")


@app.command("run-resume")
@handle_cli_errors
def run_resume(
    *,
    orchestrator: OrchestratorOption = None,
    orchestrator_run_id: Annotated[
        int,
        typer.Option("--orchestrator-run-id", help="Run ID."),
    ],
    reason: ReasonOption = None,
) -> None:
    """Resume an orchestrator run (set status to active)."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = load_orchestrator(orchestrator)
    run = orch.resume_run(orchestrator_run_id, reason=reason)
    output.print_success(
        f"Resumed run {run.id} ({run.run_key}) to status={run.status.value}."
    )


@app.command("run-cancel")
@handle_cli_errors
def run_cancel(
    *,
    orchestrator: OrchestratorOption = None,
    orchestrator_run_id: Annotated[
        int,
        typer.Option("--orchestrator-run-id", help="Run ID."),
    ],
    reason: ReasonOption = None,
) -> None:
    """Cancel an orchestrator run (set status to cancelled)."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = load_orchestrator(orchestrator)
    run = orch.cancel_run(orchestrator_run_id, reason=reason)
    output.print_warning(
        f"Cancelled run {run.id} ({run.run_key}) status={run.status.value}."
    )


@app.command("backfill-plan")
@handle_cli_errors
def backfill_plan(
    *,
    orchestrator: OrchestratorOption = None,
    start: Annotated[
        str, typer.Option("--start", help="Backfill start (ISO datetime).")
    ],
    end: Annotated[str, typer.Option("--end", help="Backfill end (ISO datetime).")],
) -> None:
    """Plan backfill run keys for a date range."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = load_orchestrator(orchestrator)
    start_dt = _parse_iso_datetime(start, option_name="--start")
    end_dt = _parse_iso_datetime(end, option_name="--end")
    run_keys = orch.plan_backfill(start_dt, end_dt)
    output.print_info(f"Planned {len(run_keys)} run key(s):")
    for key in run_keys:
        output.print_info(f"  {key}")


@app.command("backfill-enqueue")
@handle_cli_errors
def backfill_enqueue(
    *,
    orchestrator: OrchestratorOption = None,
    start: Annotated[
        str, typer.Option("--start", help="Backfill start (ISO datetime).")
    ],
    end: Annotated[str, typer.Option("--end", help="Backfill end (ISO datetime).")],
    priority: Annotated[int, typer.Option("--priority", help="Run priority.")] = 0,
    submitted_by: Annotated[
        str | None,
        typer.Option("--submitted-by", help="Who submitted this request."),
    ] = None,
    reason: ReasonOption = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Requeue runs if they already exist."),
    ] = False,
) -> None:
    """Enqueue pending backfill runs for a date range."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    orch = load_orchestrator(orchestrator)
    start_dt = _parse_iso_datetime(start, option_name="--start")
    end_dt = _parse_iso_datetime(end, option_name="--end")
    runs = orch.enqueue_backfill(
        start=start_dt,
        end=end_dt,
        priority=priority,
        submitted_by=submitted_by,
        reason=reason,
        force=force,
    )
    output.print_success(f"Enqueued {len(runs)} backfill run(s).")
    for run in runs:
        output.print_info(f"  {run.id}  {run.run_key}")


@app.command("replay-enqueue")
@handle_cli_errors
def replay_enqueue(
    *,
    orchestrator: OrchestratorOption = None,
    run_key: Annotated[
        list[str],
        typer.Option("--run-key", help="Run key to replay (repeatable)."),
    ],
    priority: Annotated[int, typer.Option("--priority", help="Run priority.")] = 0,
    submitted_by: Annotated[
        str | None,
        typer.Option("--submitted-by", help="Who submitted this request."),
    ] = None,
    reason: ReasonOption = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Requeue runs if they already exist."),
    ] = False,
) -> None:
    """Enqueue pending replay runs for explicit run keys."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )
    if not run_key:
        raise CliUserError("Provide at least one --run-key")
    orch = load_orchestrator(orchestrator)
    runs = orch.enqueue_replay(
        run_keys=run_key,
        priority=priority,
        submitted_by=submitted_by,
        reason=reason,
        force=force,
    )
    output.print_success(f"Enqueued {len(runs)} replay run(s).")
    for run in runs:
        output.print_info(f"  {run.id}  {run.run_key}")

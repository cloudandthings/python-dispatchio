from __future__ import annotations

from enum import Enum

import typer
from typing import Annotated

from dispatchio.cli import output
from dispatchio.cli.errors import handle_cli_errors
from dispatchio.cli.errors import CliUserError
from dispatchio.cli.loaders import load_orchestrator, load_store_from_context
from dispatchio.cli.options import (
    AllNamespacesOption,
    AttemptOption,
    ContextOption,
    JobOption,
    JobsOption,
    LimitOption,
    OperatorOption,
    OrchestratorOption,
    ReasonOption,
    RunKeyOption,
    RunKeyRequiredOption,
)
from dispatchio.models import Attempt, Status


app = typer.Typer(help="Create and list manual retry requests.", no_args_is_help=True)


class RetryListView(str, Enum):
    ATTEMPTS = "attempts"
    REQUESTS = "requests"


@app.command("create")
@handle_cli_errors
def retry_create(
    *,
    orchestrator: OrchestratorOption = None,
    run_key: RunKeyRequiredOption,
    jobs: JobsOption = None,
    reason: ReasonOption = None,
    requested_by: OperatorOption = "cli",
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Print selected jobs and assigned attempt numbers without writing state.",
        ),
    ] = False,
) -> None:
    """Create a manual retry for one or more jobs."""
    if not orchestrator:
        raise CliUserError(
            "Specify an orchestrator with --orchestrator or DISPATCHIO_ORCHESTRATOR"
        )

    with output.console.status("Loading orchestrator..."):
        orch = load_orchestrator(orchestrator)

    if jobs:
        target_jobs = list(jobs)
    else:
        all_attempts = orch.state.list_attempts(run_key=run_key)
        latest_by_job: dict[str, Attempt] = {}
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
        output.print_warning("No failed jobs found for this run.")
        return

    jobs_with_attempts: list[tuple[str, int]] = []
    for job_name in sorted(target_jobs):
        latest = orch.state.get_latest_attempt(job_name, run_key)
        next_attempt = (latest.attempt + 1) if latest else 0
        jobs_with_attempts.append((job_name, next_attempt))

    if dry_run:
        output.print_retry_plan(run_key, jobs_with_attempts, dry_run=True)
        return

    output.print_retry_plan(run_key, jobs_with_attempts, dry_run=False)

    for job_name in sorted(target_jobs):
        try:
            new_rec = orch.manual_retry(
                job_name,
                run_key,
                requested_by=requested_by,
                request_reason=reason or "",
            )
            output.print_success(f"  ↺ {job_name}  attempt={new_rec.attempt}")
        except ValueError as exc:
            output.print_error(f"  ✗ {job_name}  {exc}")


@app.command("list")
@handle_cli_errors
def retry_list(
    *,
    context_name: ContextOption = None,
    run_key: RunKeyOption = None,
    job: JobOption = None,
    attempt: AttemptOption = None,
    view: Annotated[
        RetryListView,
        typer.Option(
            "--filter",
            help="'attempts' shows execution history; 'requests' shows manual retry audit log.",
            show_default=True,
            case_sensitive=False,
        ),
    ] = RetryListView.ATTEMPTS,
    limit: LimitOption = 50,
    all_namespaces: AllNamespacesOption = False,
) -> None:
    """List attempt history or manual retry requests."""
    store = load_store_from_context(context_name, all_namespaces=all_namespaces)

    if view == RetryListView.REQUESTS:
        requests = store.list_retry_requests(run_key=run_key)
        if job:
            requests = [request for request in requests if job in request.selected_jobs]
        requests = requests[:limit]
        if not requests:
            output.print_warning("No retry requests found.")
            return
        output.print_retry_requests(requests)
        return

    records = store.list_attempts(
        job_name=job,
        run_key=run_key,
        attempt=attempt,
    )
    records = records[:limit]
    if not records:
        output.print_warning("No attempts found.")
        return
    output.print_records(records)


@app.callback()
def retry_callback() -> None:
    """Create and list manual retry requests."""

from __future__ import annotations

from uuid import uuid4

import typer
from typing import Annotated

from dispatchio.cli import output
from dispatchio.cli.errors import handle_cli_errors
from dispatchio.cli.loaders import load_store_from_context
from dispatchio.cli.options import ContextOption, ReasonOption, YesOption
from dispatchio.models import AttemptRecord, Status


app = typer.Typer(help="Manually inspect or override run records.")


@app.command("set")
@handle_cli_errors
def record_set(
    job_name: Annotated[str, typer.Argument(help="Job name.")],
    run_key: Annotated[str, typer.Argument(help="Run key.")],
    new_status: Annotated[
        Status,
        typer.Argument(metavar="STATUS", help="New status value."),
    ],
    *,
    context_name: ContextOption = None,
    reason: ReasonOption = None,
    yes: YesOption = False,
) -> None:
    """Manually set the status of a run record."""
    store = load_store_from_context(context_name)

    if not yes and not output.confirm(
        f"Set {job_name}[{run_key}] -> {new_status.value}?"
    ):
        raise typer.Exit()

    existing = store.get_latest_attempt(job_name, run_key)
    if existing is None:
        record = AttemptRecord(
            job_name=job_name,
            run_key=run_key,
            attempt=0,
            correlation_id=uuid4(),
            status=new_status,
            reason=reason,
        )
        store.append_attempt(record)
    else:
        record = existing.model_copy(update={"status": new_status, "reason": reason})
        store.update_attempt(record)

    output.print_success(f"Set {job_name}[{run_key}] -> {new_status.value}")


@app.callback()
def record_callback() -> None:
    """Manually inspect or override run records."""

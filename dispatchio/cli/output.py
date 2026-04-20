from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.table import Table
from rich.text import Text

from dispatchio.contexts import ContextEntry
from dispatchio.models import AttemptRecord, RetryRequest, TickResult
from dispatchio.tick_log import TickLogRecord


console = Console(highlight=False)
error_console = Console(stderr=True, highlight=False)

_STATUS_COLOURS: dict[str, str] = {
    "done": "green",
    "submitted": "cyan",
    "would_submit": "cyan",
    "would_defer": "yellow",
    "queued": "cyan",
    "running": "blue",
    "error": "red",
    "lost": "yellow",
    "cancelled": "yellow",
    "deferred_active_limit": "yellow",
    "deferred_pool_active_limit": "yellow",
    "deferred_submit_limit": "yellow",
    "deferred_pool_submit_limit": "yellow",
}

_ACTION_ICONS: dict[str, str] = {
    "submitted": "✓",
    "would_submit": "→",
    "would_defer": "⧗",
    "retrying": "↺",
    "marked_lost": "✗",
    "marked_error": "✗",
    "submission_failed": "✗",
    "deferred_active_limit": "⧗",
    "deferred_pool_active_limit": "⧗",
    "deferred_submit_limit": "⧗",
    "deferred_pool_submit_limit": "⧗",
    "skipped_condition": "·",
    "skipped_dependencies": "·",
    "skipped_already_active": "·",
    "skipped_already_done": "·",
}


def print_error(message: str) -> None:
    error_console.print(f"[red]Error:[/red] {message}")


def print_success(message: str) -> None:
    console.print(f"[green]{message}[/green]")


def print_warning(message: str) -> None:
    console.print(f"[yellow]{message}[/yellow]")


def print_info(message: str) -> None:
    console.print(message)


def print_retry_plan(
    run_id: str, jobs_with_attempts: list[tuple[str, int]], *, dry_run: bool
) -> None:
    prefix = "[dry-run] Would retry" if dry_run else "Retrying"
    print_info(f"{prefix} {len(jobs_with_attempts)} job(s) for run {run_id}:")
    for job_name, attempt in jobs_with_attempts:
        print_info(f"  {job_name}  attempt={attempt}")


def print_graph_summary(
    path: str,
    *,
    orchestrator_name: str,
    graph_version: str,
    job_count: int,
    external_dependency_count: int,
    producer_name: str | None,
    producer_version: str | None,
) -> None:
    print_success(f"Graph {path} is valid.")
    print_info(f"  orchestrator : {orchestrator_name}")
    print_info(f"  graph_version: {graph_version}")
    print_info(f"  jobs         : {job_count}")
    if external_dependency_count:
        print_info(f"  external deps: {external_dependency_count}")
    if producer_name and producer_version:
        print_info(f"  producer     : {producer_name} {producer_version}")


def print_records(records: list[AttemptRecord]) -> None:
    table = Table(show_header=True, header_style="bold")
    table.add_column("JOB")
    table.add_column("RUN_ID")
    table.add_column("STATUS")
    table.add_column("ATTEMPT", justify="right")
    table.add_column("COMPLETED")

    for record in records:
        status_value = record.status.value
        status_colour = _STATUS_COLOURS.get(status_value, "white")
        completed = record.completed_at.isoformat() if record.completed_at else "-"
        table.add_row(
            record.job_name,
            record.logical_run_id,
            f"[{status_colour}]{status_value}[/{status_colour}]",
            str(record.attempt),
            completed,
        )

    console.print(table)


def print_tick_result(result: TickResult) -> None:
    if not result.results:
        console.print(
            Panel(Text("No actions taken."), title=result.reference_time.isoformat())
        )
        return

    lines: list[Text] = []
    for item in result.results:
        action = item.action.value
        icon = _ACTION_ICONS.get(action, "?")
        colour = _STATUS_COLOURS.get(action, "white")
        line = Text(f"{icon} {item.job_name}[{item.run_id}] -> {action}", style=colour)
        if item.detail:
            line.append(f"  {item.detail}")
        lines.append(line)

    block = Text()
    for idx, line in enumerate(lines):
        block.append_text(line)
        if idx < len(lines) - 1:
            block.append("\n")
    console.print(Panel(block, title=result.reference_time.isoformat()))


def print_tick_summary(record: TickLogRecord, *, detail: bool = False) -> None:
    submitted = sum(
        1 for action in record.actions if action.get("action") == "submitted"
    )
    total = len(record.actions)
    summary = f"{total} action(s)"
    if submitted:
        summary += f" ({submitted} submitted)"

    console.print(
        f"{record.ticked_at}  ref={record.reference_time[:10]}  "
        f"{record.duration_seconds:.2f}s  {summary}"
    )

    if not detail:
        return

    for action in record.actions:
        action_name = str(action.get("action", ""))
        icon = _ACTION_ICONS.get(action_name, "?")
        job_name = action.get("job_name", "?")
        run_id = action.get("run_id", "?")
        action_detail = action.get("detail", "")
        suffix = f"  {action_detail}" if action_detail else ""
        console.print(f"  {icon} {job_name}[{run_id}] -> {action_name}{suffix}")


def print_retry_requests(requests: list[RetryRequest]) -> None:
    table = Table(show_header=True, header_style="bold")
    table.add_column("REQUESTED_AT")
    table.add_column("BY")
    table.add_column("RUN_ID")
    table.add_column("JOBS")

    for request in requests:
        table.add_row(
            request.requested_at.isoformat(),
            request.requested_by,
            request.logical_run_id,
            ", ".join(request.selected_jobs),
        )

    console.print(table)


def print_context_list(entries: list[ContextEntry], current: str | None) -> None:
    table = Table(show_header=True, header_style="bold")
    table.add_column("")
    table.add_column("NAME")
    table.add_column("CONFIG PATH")
    table.add_column("DESCRIPTION")

    for entry in entries:
        marker = "*" if entry.name == current else ""
        table.add_row(marker, entry.name, entry.config_path, entry.description)

    console.print(table)


def print_json(data: str) -> None:
    console.print_json(data)


def confirm(message: str) -> bool:
    return Confirm.ask(message, console=console)

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from dispatchio.models import Status


def _validate_existing_path(value: Path) -> Path:
    if not value.exists():
        raise typer.BadParameter(f"Path does not exist: {value}")
    return value


OrchestratorOption = Annotated[
    str | None,
    typer.Option(
        "--orchestrator",
        "-o",
        envvar="DISPATCHIO_ORCHESTRATOR",
        help="module:attribute path to an Orchestrator, e.g. myproject.jobs:orchestrator",
        show_default=False,
    ),
]

ContextOption = Annotated[
    str | None,
    typer.Option(
        "--context",
        "-c",
        help="Named context from ~/.dispatchio/contexts.json. Run 'dispatchio context list'.",
        show_default=False,
    ),
]

ReferenceTimeOption = Annotated[
    str | None,
    typer.Option(
        "--reference-time",
        "-t",
        help="ISO-8601 datetime to use as the tick reference time. Defaults to now (UTC).",
        show_default=False,
    ),
]

RunKeyOption = Annotated[
    str | None,
    typer.Option("--run-key", "-r", help="Filter by run key.", show_default=False),
]

RunKeyRequiredOption = Annotated[
    str,
    typer.Option("--run-key", "-r", help="Run key."),
]

JobOption = Annotated[
    str | None,
    typer.Option("--job", "-j", help="Filter by job name.", show_default=False),
]

JobRequiredOption = Annotated[
    str,
    typer.Option("--job", "-j", help="Job name."),
]

JobsOption = Annotated[
    list[str] | None,
    typer.Option(
        "--job",
        "-j",
        help="Job name (repeatable). If omitted, retries all failed jobs.",
        show_default=False,
    ),
]

AttemptOption = Annotated[
    int | None,
    typer.Option("--attempt", "-a", help="Attempt number.", show_default=False),
]

LimitOption = Annotated[
    int,
    typer.Option("--limit", "-n", help="Max rows to show.", show_default=True),
]

ReasonOption = Annotated[
    str | None,
    typer.Option("--reason", help="Reason string.", show_default=False),
]

OperatorOption = Annotated[
    str,
    typer.Option("--operator", help="Name/ID of the operator.", show_default=True),
]

StatusFilterOption = Annotated[
    Status | None,
    typer.Option(
        "--status",
        help="Filter by status.",
        case_sensitive=False,
        show_default=False,
    ),
]

YesOption = Annotated[
    bool,
    typer.Option("--yes", "-y", help="Skip confirmation prompt.", show_default=False),
]

EntryPointArgument = Annotated[
    str,
    typer.Argument(help="module:function entry point, e.g. mypackage.jobs:run_ingest"),
]

ScriptPathArgument = Annotated[
    Path,
    typer.Argument(
        callback=_validate_existing_path, help="Path to a Python script file."
    ),
]

FunctionNameArgument = Annotated[
    str,
    typer.Argument(help="Function name within the script."),
]

GraphPathArgument = Annotated[
    Path,
    typer.Argument(
        callback=_validate_existing_path, help="Path to graph artifact file."
    ),
]

AllNamespacesOption = Annotated[
    bool,
    typer.Option(
        "--all-namespaces",
        "-A",
        help="Show records from all namespaces instead of the context's configured namespace.",
        show_default=False,
    ),
]

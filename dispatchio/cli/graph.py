from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Annotated

import typer

from dispatchio.cli import output
from dispatchio.cli.errors import handle_cli_errors
from dispatchio.cli.errors import CliUserError
from dispatchio.cli.options import GraphPathArgument


app = typer.Typer(help="Graph artifact utilities (JSON graph mode).")


@app.command("validate")
@handle_cli_errors
def graph_validate(path: GraphPathArgument) -> None:
    """Validate a graph artifact file (Pydantic + graph-level checks)."""
    from dispatchio.graph import GraphValidationError, load_graph, validate_graph

    try:
        spec = load_graph(path)
    except GraphValidationError as exc:
        raise CliUserError(str(exc)) from exc

    try:
        validate_graph(spec)
    except GraphValidationError as exc:
        raise CliUserError(str(exc)) from exc

    output.print_graph_summary(
        str(path),
        namespace=spec.name,
        graph_version=str(spec.graph_version),
        job_count=len(spec.jobs),
        external_dependency_count=len(spec.external_dependencies or []),
        producer_name=spec.producer.name if spec.producer else None,
        producer_version=spec.producer.version if spec.producer else None,
    )


@app.command("schema")
@handle_cli_errors
def graph_schema(
    *,
    output_path: Annotated[
        str | None,
        typer.Option(
            "--output",
            "-o",
            help="Write schema to FILE instead of stdout.",
            metavar="FILE",
        ),
    ] = None,
) -> None:
    """Print the JSON Schema for GraphSpec."""
    from dispatchio.graph import dump_schema

    schema_str = _json.dumps(dump_schema(), indent=2)
    if output_path:
        Path(output_path).write_text(schema_str)
        output.print_success(f"Schema written to {output_path}")
        return

    output.print_json(schema_str)


@app.callback()
def graph_callback() -> None:
    """Graph artifact utilities (JSON graph mode)."""

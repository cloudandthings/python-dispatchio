from __future__ import annotations

from pathlib import Path

import typer
from typing import Annotated

from dispatchio.cli import output
from dispatchio.cli.errors import handle_cli_errors
from dispatchio.cli.errors import CliUserError


app = typer.Typer(help="Manage named orchestrator contexts (config file pointers).", no_args_is_help=True)


@app.command("add")
@handle_cli_errors
def context_add(
    name: Annotated[str, typer.Argument(help="Context name.")],
    config_path: Annotated[str, typer.Argument(help="Path to config file.")],
    *,
    description: Annotated[
        str,
        typer.Option("--description", "-d", help="Optional description."),
    ] = "",
) -> None:
    """Register a named context pointing to CONFIG_PATH."""
    from dispatchio.contexts import ContextEntry, ContextStore

    abs_path = str(Path(config_path).expanduser().resolve())
    if not Path(abs_path).exists():
        raise CliUserError(f"Config file not found: {abs_path}")

    ContextStore().add(
        ContextEntry(name=name, config_path=abs_path, description=description)
    )
    output.print_success(f"Added context {name!r} -> {abs_path}")


@app.command("list")
@handle_cli_errors
def context_list() -> None:
    """List all registered contexts."""
    from dispatchio.contexts import ContextStore

    store = ContextStore()
    entries = store.list()
    current = store.current_name()

    if not entries:
        output.print_warning(
            "No contexts registered. Use 'dispatchio context add' to register one."
        )
        return

    output.print_context_list(entries, current)


@app.command("use")
@handle_cli_errors
def context_use(name: Annotated[str, typer.Argument(help="Context name.")]) -> None:
    """Set NAME as the active context for subsequent commands."""
    from dispatchio.contexts import ContextStore

    try:
        ContextStore().use(name)
    except KeyError as exc:
        raise CliUserError(str(exc)) from exc

    output.print_success(f"Switched to context {name!r}.")


@app.command("remove")
@handle_cli_errors
def context_remove(name: Annotated[str, typer.Argument(help="Context name.")]) -> None:
    """Remove a registered context (does not delete any files)."""
    from dispatchio.contexts import ContextStore

    ContextStore().remove(name)
    output.print_success(f"Removed context {name!r}.")


@app.callback()
def context_callback() -> None:
    """Manage named orchestrator contexts (config file pointers)."""

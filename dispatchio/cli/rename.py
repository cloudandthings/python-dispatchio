from __future__ import annotations

import typer
from typing import Annotated

from dispatchio.cli.errors import CliUserError, handle_cli_errors
from dispatchio.cli.loaders import load_store_from_context
from dispatchio.cli.options import ContextOption

job_app = typer.Typer(help="Manage job identity in the registry.", no_args_is_help=True)
namespace_app = typer.Typer(help="Manage namespace identity in the registry.", no_args_is_help=True)
event_app = typer.Typer(help="Manage event type identity in the registry.", no_args_is_help=True)

_NameArg = Annotated[str, typer.Argument()]
_NamespaceOpt = Annotated[
    str | None,
    typer.Option("--namespace", "-n", help="Target namespace (overrides context default)."),
]


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


@job_app.command("rename")
@handle_cli_errors
def job_rename(
    old_name: _NameArg,
    new_name: _NameArg,
    *,
    namespace: _NamespaceOpt = None,
    context_name: ContextOption = None,
) -> None:
    """Rename a job in the registry.

    Run BEFORE renaming the job in code. All history linked to the old name
    will resolve to the new name automatically after this command completes.
    """
    store = _load(context_name, namespace)
    try:
        store.rename_job(old_name, new_name)
    except ValueError as exc:
        raise CliUserError(str(exc)) from exc
    typer.echo(f"Job renamed: {old_name!r} → {new_name!r}")


@job_app.command("remap")
@handle_cli_errors
def job_remap(
    old_name: _NameArg,
    new_name: _NameArg,
    *,
    namespace: _NamespaceOpt = None,
    context_name: ContextOption = None,
) -> None:
    """Remap an orphaned job entry after the code was renamed before the registry.

    Use this when code was renamed first and the old registry entry is now
    orphaned (no live job registration matches it).
    """
    store = _load(context_name, namespace)
    try:
        store.remap_job(old_name, new_name)
    except ValueError as exc:
        raise CliUserError(str(exc)) from exc
    typer.echo(f"Job remapped: {old_name!r} → {new_name!r}")


@job_app.callback()
def job_callback() -> None:
    """Manage job identity in the registry."""


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


@namespace_app.command("rename")
@handle_cli_errors
def namespace_rename(
    old_name: _NameArg,
    new_name: _NameArg,
    *,
    context_name: ContextOption = None,
) -> None:
    """Rename a namespace in the registry.

    Run BEFORE updating the namespace in code or config.
    """
    store = load_store_from_context(context_name, all_namespaces=True)
    try:
        store.rename_namespace(old_name, new_name)
    except ValueError as exc:
        raise CliUserError(str(exc)) from exc
    typer.echo(f"Namespace renamed: {old_name!r} → {new_name!r}")


@namespace_app.command("remap")
@handle_cli_errors
def namespace_remap(
    old_name: _NameArg,
    new_name: _NameArg,
    *,
    context_name: ContextOption = None,
) -> None:
    """Remap an orphaned namespace entry after config was changed before the registry."""
    store = load_store_from_context(context_name, all_namespaces=True)
    try:
        store.remap_namespace(old_name, new_name)
    except ValueError as exc:
        raise CliUserError(str(exc)) from exc
    typer.echo(f"Namespace remapped: {old_name!r} → {new_name!r}")


@namespace_app.callback()
def namespace_callback() -> None:
    """Manage namespace identity in the registry."""


# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------


@event_app.command("rename")
@handle_cli_errors
def event_rename(
    old_name: _NameArg,
    new_name: _NameArg,
    *,
    namespace: _NamespaceOpt = None,
    context_name: ContextOption = None,
) -> None:
    """Rename an event type in the registry.

    Run BEFORE renaming the event in code.
    """
    store = _load(context_name, namespace)
    try:
        store.rename_event(old_name, new_name)
    except ValueError as exc:
        raise CliUserError(str(exc)) from exc
    typer.echo(f"Event renamed: {old_name!r} → {new_name!r}")


@event_app.command("remap")
@handle_cli_errors
def event_remap(
    old_name: _NameArg,
    new_name: _NameArg,
    *,
    namespace: _NamespaceOpt = None,
    context_name: ContextOption = None,
) -> None:
    """Remap an orphaned event type entry after code was renamed before the registry."""
    store = _load(context_name, namespace)
    try:
        store.remap_event(old_name, new_name)
    except ValueError as exc:
        raise CliUserError(str(exc)) from exc
    typer.echo(f"Event remapped: {old_name!r} → {new_name!r}")


@event_app.callback()
def event_callback() -> None:
    """Manage event type identity in the registry."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load(context_name: str | None, namespace: str | None):
    """Load the state store, optionally overriding the namespace."""
    from dispatchio.config.loader import _build_state, load_config
    from dispatchio.contexts import ContextStore

    entry = ContextStore().resolve(context_name)
    if entry is not None:
        try:
            settings = load_config(entry.config_path)
            ns = namespace if namespace is not None else settings.namespace
            return _build_state(settings.state, namespace=ns)
        except Exception as exc:
            raise CliUserError(
                f"Failed loading state for context {entry.name!r}: {exc}"
            ) from exc

    try:
        settings = load_config()
        if settings is not None:
            ns = namespace if namespace is not None else settings.namespace
            return _build_state(settings.state, namespace=ns)
    except Exception as exc:
        raise CliUserError(f"Failed loading default Dispatchio config: {exc}") from exc

    raise CliUserError(
        "Cannot locate state store. Use --context or run from a directory with dispatchio.toml."
    )

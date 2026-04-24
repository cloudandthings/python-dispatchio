from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

from dispatchio.cli.errors import CliUserError
from dispatchio.orchestrator import Orchestrator
from dispatchio.state import SQLAlchemyStateStore
from dispatchio.tick_log import FilesystemTickLogStore


def load_orchestrator(path: str) -> Orchestrator:
    if ":" not in path:
        raise CliUserError(
            f"Orchestrator path must be 'module:attribute', got: {path!r}"
        )

    module_path, attr = path.rsplit(":", 1)
    sys.path.insert(0, os.getcwd())
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        raise CliUserError(f"Cannot import module {module_path!r}: {exc}") from exc

    obj = getattr(module, attr, None)
    if obj is None:
        raise CliUserError(f"Attribute {attr!r} not found in module {module_path!r}")

    if not isinstance(obj, Orchestrator):
        raise CliUserError(f"{path!r} is a {type(obj).__name__}, expected Orchestrator")

    return obj


def load_store_from_context(
    context_name: str | None, *, all_namespaces: bool = False
) -> SQLAlchemyStateStore:
    from dispatchio.config.loader import _build_state, load_config
    from dispatchio.contexts import ContextStore

    entry = ContextStore().resolve(context_name)
    if entry is not None:
        try:
            settings = load_config(entry.config_path)
            namespace = None if all_namespaces else getattr(settings, "namespace", "default")
            return _build_state(settings.state, namespace=namespace)  # type: ignore[return-value]
        except Exception as exc:
            raise CliUserError(
                f"Failed loading state for context {entry.name!r}: {exc}"
            ) from exc

    try:
        settings = load_config()
        if settings is not None:
            namespace = None if all_namespaces else getattr(settings, "namespace", "default")
            return _build_state(settings.state, namespace=namespace)  # type: ignore[return-value]
    except Exception as exc:
        raise CliUserError(f"Failed loading default Dispatchio config: {exc}") from exc

    raise CliUserError(
        "Cannot locate state store. Use --context or run from a directory with dispatchio.toml."
    )


def resolve_tick_log_store(context_name: str | None) -> FilesystemTickLogStore:
    from dispatchio.config.loader import load_config
    from dispatchio.contexts import ContextStore

    entry = ContextStore().resolve(context_name)
    if entry is not None:
        try:
            settings = load_config(entry.config_path)
            return FilesystemTickLogStore(Path(settings.state.tick_log_path))
        except Exception as exc:
            raise CliUserError(
                f"Failed loading tick log for context {entry.name!r}: {exc}"
            ) from exc

    try:
        settings = load_config()
        if settings is not None:
            return FilesystemTickLogStore(Path(settings.state.tick_log_path))
    except Exception as exc:
        raise CliUserError(f"Failed loading default Dispatchio config: {exc}") from exc

    raise CliUserError(
        "Cannot locate tick log. Use --context or run from a directory with dispatchio.toml."
    )

"""CLI-specific exception types and error-handling decorator."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

import typer


class CliUserError(Exception):
    """Raised for user-facing CLI failures. Caught by @handle_cli_errors."""


def handle_cli_errors(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that catches CliUserError and exits with a user-facing message."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        from dispatchio.cli import output  # local import avoids cycle with output.py

        try:
            return fn(*args, **kwargs)
        except CliUserError as exc:
            output.print_error(str(exc))
            raise typer.Exit(code=1) from exc

    return wrapper

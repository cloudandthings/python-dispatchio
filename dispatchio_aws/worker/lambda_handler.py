from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from beartype import beartype

from dispatchio.worker.harness import run_job
from dispatchio.worker.reporter.base import BaseReporter

P = ParamSpec("P")
R = TypeVar("R")


@beartype
def dispatchio_handler(
    fn: Callable[P, R] | None = None,
    *,
    job_name: str | None = None,
    reporter: BaseReporter | None = None,
) -> Callable[..., dict[str, Any]]:
    """
    Decorator for Lambda functions that should report completion to dispatchio.

    The wrapped lambda function expects an event containing run_key and receives
    context injection through dispatchio.run_job.
    """

    def _decorate(target: Callable[..., Any]) -> Callable[..., dict[str, Any]]:
        resolved_job_name = job_name or target.__name__

        @functools.wraps(target)
        def _wrapped(event: dict[str, Any], context: Any) -> dict[str, Any]:
            run_key = event.get("run_key")
            if not run_key:
                raise ValueError("Lambda event must include 'run_key'")

            def _invoke(**kwargs: Any) -> None:
                target(**kwargs)

            run_job(
                resolved_job_name,
                _invoke,
                run_key=run_key,
                reporter=reporter,
            )
            return {
                "status": "ok",
                "job_name": resolved_job_name,
                "run_key": run_key,
            }

        return _wrapped

    if fn is not None:
        return _decorate(fn)

    return _decorate

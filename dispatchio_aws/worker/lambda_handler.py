from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from beartype import beartype

from dispatchio.worker.harness import run_job
from dispatchio.worker.reporter.base import Reporter

P = ParamSpec("P")
R = TypeVar("R")


@beartype
def dispatchio_handler(
    fn: Callable[P, R] | None = None,
    *,
    job_name: str | None = None,
    reporter: Reporter | None = None,
) -> Callable[..., dict[str, Any]]:
    """
    Decorator for Lambda functions that should report completion to dispatchio.

    The wrapped lambda function expects an event containing run_id and receives
    context injection through dispatchio.run_job.
    """

    def _decorate(target: Callable[..., Any]) -> Callable[..., dict[str, Any]]:
        resolved_job_name = job_name or target.__name__

        @functools.wraps(target)
        def _wrapped(event: dict[str, Any], context: Any) -> dict[str, Any]:
            run_id = event.get("run_id")
            if not run_id:
                raise ValueError("Lambda event must include 'run_id'")

            def _invoke(**kwargs: Any) -> None:
                target(**kwargs)

            run_job(
                resolved_job_name,
                _invoke,
                run_id=run_id,
                reporter=reporter,
            )
            return {"status": "ok", "job_name": resolved_job_name, "run_id": run_id}

        return _wrapped

    if fn is not None:
        return _decorate(fn)

    return _decorate

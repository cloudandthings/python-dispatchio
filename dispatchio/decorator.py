"""
@job decorator — co-locate Dispatchio job metadata with the function.

The decorated function is transparent at call time; the Job definition is
attached as ``fn._dispatchio_job`` for discovery by ``dispatchio run-file``.

Usage::

    from dispatchio import job, DAILY

    @job(cadence=DAILY)
    def extract(run_key: str) -> None:
        ...

    @job(cadence=DAILY, depends_on=extract)
    def transform(run_key: str) -> None:
        ...

``depends_on`` accepts a decorated function (resolved to its Job), a ``Job``
object, a ``Dependency``, or a list of any combination.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable

from dispatchio.cadence import Cadence
from dispatchio.models import Job, PythonJob


def _resolve_depends_on(depends_on: Any) -> list:
    if depends_on is None:
        return []
    if not isinstance(depends_on, (list, tuple)):
        depends_on = [depends_on]
    resolved = []
    for dep in depends_on:
        if hasattr(dep, "_dispatchio_job"):
            resolved.append(dep._dispatchio_job)
        else:
            resolved.append(dep)
    return resolved


class job:  # noqa: N801 — lowercase intentional for decorator use
    """
    Decorator that registers a function as a Dispatchio job.

    Args:
        cadence: Run cadence (e.g. ``DAILY``, ``MONTHLY``). If omitted the
                 orchestrator's ``default_cadence`` applies.
        depends_on: A ``@job``-decorated function, a ``Job``, a ``Dependency``,
                    or a list of any combination. Dependency run keys are
                    resolved against the same orchestrator context.
        name: Job name override. Defaults to the function name.
        **kwargs: Additional ``Job`` fields (``retry_policy``, ``condition``,
                  ``pool``, ``priority``, ``alerts``, etc.).
    """

    def __init__(
        self,
        *,
        cadence: Cadence | None = None,
        depends_on: Any = None,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._cadence = cadence
        self._depends_on = depends_on
        self._name = name
        self._kwargs = kwargs

    def __call__(self, fn: Callable) -> Callable:
        script = inspect.getfile(fn)
        job_name = self._name or fn.__name__
        resolved_deps = _resolve_depends_on(self._depends_on)

        job_def = Job.create(
            job_name,
            executor=PythonJob(script=script, function=fn.__name__),
            cadence=self._cadence,
            depends_on=resolved_deps or None,
            **self._kwargs,
        )

        @functools.wraps(fn)
        def wrapper(*args: Any, **kw: Any) -> Any:
            return fn(*args, **kw)

        wrapper._dispatchio_job = job_def  # type: ignore[attr-defined]
        return wrapper

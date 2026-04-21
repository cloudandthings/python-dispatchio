"""Decorator helpers for DataStore convenience patterns."""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, cast

from beartype import beartype

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


@beartype
def dispatchio_write_results(
    fn: Callable[P, R] | None = None,
    *,
    key: str = "return_value",
    namespace: str | None = None,
    strict: bool = True,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator that writes the wrapped function's return value to DataStore.

    The store is resolved via get_data_store(), so job and run_key can be
    sourced from worker env vars (DISPATCHIO_JOB_NAME / DISPATCHIO_RUN_KEY).

    Args:
        fn: Optional function for bare decorator usage.
        key: DataStore key to write to.
        namespace: Optional DataStore namespace override.
        strict: When True, DataStore errors are raised. When False, DataStore
            errors are logged and ignored.
    """

    def _decorate(target: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(target)
        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            result = target(*args, **kwargs)
            try:
                from dispatchio.datastore import get_data_store

                store = get_data_store(namespace=namespace)
                store.write(result, key=key)
            except Exception:
                if strict:
                    raise
                logger.warning(
                    "Unable to persist return value to DataStore for %s",
                    target.__name__,
                    exc_info=True,
                )
            return result

        return _wrapped

    if fn is not None:
        return _decorate(fn)
    return cast(Callable[[Callable[P, R]], Callable[P, R]], _decorate)


@beartype
def dispatchio_read_results(
    fn: Callable[P, R] | None = None,
    *,
    param: str,
    job: str,
    key: str = "return_value",
    run_key: str | None = None,
    namespace: str | None = None,
    strict: bool = True,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator that reads a DataStore value and injects it into one parameter.

    If the caller already provided the target parameter in kwargs, the injected
    value is skipped and the explicit caller-provided value is used.

    Args:
        fn: Optional function for bare decorator usage.
        param: Parameter name to inject.
        job: Producer job name to read from.
        key: DataStore key to read.
        run_key: Optional run_key override. If omitted, resolved from env.
        namespace: Optional DataStore namespace override.
        strict: When True, DataStore errors are raised. When False, DataStore
            errors are logged and ignored; injected value becomes None.
    """

    def _decorate(target: Callable[P, R]) -> Callable[P, R]:
        sig = inspect.signature(target)
        accepts_kwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
        if not accepts_kwargs and param not in sig.parameters:
            raise ValueError(
                f"dispatchio_read_results(param={param!r}) cannot be applied to "
                f"{target.__name__} because that parameter is not in its signature"
            )

        @functools.wraps(target)
        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if param not in kwargs:
                value: Any = None
                try:
                    from dispatchio.datastore import get_data_store

                    store = get_data_store(namespace=namespace)
                    value = store.read(job=job, run_key=run_key, key=key)
                except Exception:
                    if strict:
                        raise
                    logger.warning(
                        "Unable to read DataStore value for %s (job=%s key=%s)",
                        target.__name__,
                        job,
                        key,
                        exc_info=True,
                    )
                kwargs[param] = value
            return target(*args, **kwargs)

        return _wrapped

    if fn is not None:
        return _decorate(fn)
    return cast(Callable[[Callable[P, R]], Callable[P, R]], _decorate)

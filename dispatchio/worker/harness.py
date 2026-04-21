"""
run_job() — the Dispatchio job harness.

This is the single entry point job scripts should call. It handles all
the Dispatchio-specific concerns so job code stays pure business logic:

  - Parses --run-key (and --drop-dir for the default filesystem reporter)
    from sys.argv, or accepts them as explicit arguments.
  - Runs the job function inside a try/except.
  - Always posts a completion event (DONE or ERROR) — even if the job
    raises an unexpected exception.

The job function receives only what its signature declares — nothing is
injected unless the function asks for it by name. Available context keys:

    run_key    — the run key string for this execution  (e.g. "20260414")
    job_name  — the registered job name               (e.g. "ingest")

Usage examples:

    # Pass nothing — function does not need orchestrator context
    def main() -> None:
        ...

    # Pass run_key only
    def main(run_key: str) -> None:
        print(f"Processing {run_key}")
        ...

    # Pass everything via **kwargs
    def main(**ctx) -> None:
        print(ctx["run_key"], ctx["job_name"])
        ...

    if __name__ == "__main__":
        run_job("my_etl_job", main)

The harness reads --run-key and --drop-dir from sys.argv automatically.
Override via explicit arguments or env vars for testing.

Environment variables (all optional):
    DISPATCHIO_RUN_KEY          fallback if --run-key not in argv
    DISPATCHIO_DROP_DIR         fallback if --drop-dir not in argv (filesystem reporter)
    DISPATCHIO_CORRELATION_ID   attempt UUID injected by executor
"""

from __future__ import annotations

import inspect
import logging
import os
import sys
from collections.abc import Callable
from typing import Any
from uuid import UUID

from dispatchio.models import Status
from dispatchio.worker.reporter.base import BaseReporter
from dispatchio.worker.reporter.filesystem import FilesystemReporter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing helpers
# ---------------------------------------------------------------------------


def _arg_from_argv(flag: str) -> str | None:
    """Extract the value after `flag` in sys.argv, e.g. --run-key 20250115."""
    argv = sys.argv
    try:
        idx = argv.index(flag)
        return argv[idx + 1]
    except (ValueError, IndexError):
        return None


def _resolve_run_key(run_key: str | None) -> str:
    value = (
        run_key or _arg_from_argv("--run-key") or os.environ.get("DISPATCHIO_RUN_KEY")
    )
    if not value:
        raise RuntimeError(
            "run_key is required. Pass it explicitly to run_job(), "
            "via --run-key in argv, or via DISPATCHIO_RUN_KEY env var."
        )
    return value


def _resolve_reporter(reporter: BaseReporter | None) -> BaseReporter | None:
    """
    Auto-detect a reporter if none is provided.
    Returns None (with a warning) if no reporter can be configured —
    the harness will still run the job but cannot signal completion.
    """
    if reporter is not None:
        return reporter

    drop_dir = _arg_from_argv("--drop-dir") or os.environ.get("DISPATCHIO_DROP_DIR")
    if drop_dir:
        return FilesystemReporter(drop_dir)

    queue_url = os.environ.get("DISPATCHIO_RECEIVER__QUEUE_URL") or os.environ.get(
        "DISPATCHIO_SQS_QUEUE_URL"
    )
    if queue_url:
        region = os.environ.get("DISPATCHIO_RECEIVER__REGION") or os.environ.get(
            "DISPATCHIO_SQS_REGION"
        )
        try:
            from dispatchio_aws.reporter.sqs import SQSReporter  # type: ignore[import]

            return SQSReporter(queue_url=queue_url, region=region)
        except ImportError:
            logger.warning(
                "SQS reporter requested but dispatchio[aws] is not installed. "
                "Install with: pip install dispatchio[aws]"
            )

    logger.warning(
        "No reporter configured. Status events will NOT be sent to Dispatchio. "
        "Pass a reporter to run_job(), use --drop-dir, or set DISPATCHIO_DROP_DIR."
    )
    return None


# ---------------------------------------------------------------------------
# Context injection
# ---------------------------------------------------------------------------


def _call_fn(fn: Callable[..., None], context: dict[str, Any]) -> None:
    """
    Call fn, injecting from context only what its signature declares.

    - No parameters        → fn()
    - Named parameters     → matched by name from context; unknown required
                             parameters raise TypeError with a clear message
    - **kwargs parameter   → fn(**context) — receives everything
    """
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        fn()
        return

    params = sig.parameters

    if not params:
        fn()
        return

    for p in params.values():
        if p.kind == inspect.Parameter.VAR_KEYWORD:
            fn(**context)
            return

    injected: dict[str, Any] = {}
    for name, param in params.items():
        if param.kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            continue
        if name in context:
            injected[name] = context[name]
        elif param.default is inspect.Parameter.empty:
            raise TypeError(
                f"Job function requires parameter {name!r} which is not available "
                f"in the execution context. Available keys: {sorted(context)}"
            )

    fn(**injected)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_job(
    job_name: str,
    fn: Callable[..., None],
    *,
    run_key: str | None = None,
    reporter: BaseReporter | None = None,
    metadata_fn: Callable[[], dict[str, Any]] | None = None,
) -> None:
    """
    Execute fn with full Dispatchio lifecycle management.

    fn receives only what its signature declares — see module docstring for
    the available context keys and usage examples.

    Args:
        job_name:           Must match the Job name exactly.
        fn:                 The job function. Declare parameters by name to
                            receive context values (run_key, job_name). Raise
                            any exception to signal failure.
        run_key:        The run_key for this execution. If None, resolved
                            from --run-key argv or DISPATCHIO_RUN_KEY env var.
        reporter:           How to send completion events back to Dispatchio.
                            Defaults to FilesystemReporter if --drop-dir or
                            DISPATCHIO_DROP_DIR is set, otherwise a warning is
                            logged and no event is sent.
        metadata_fn:        Optional callable returning a dict of metadata to
                            attach to the DONE event. Called after fn() returns
                            successfully.
    """
    resolved_run_key = _resolve_run_key(run_key)
    resolved_reporter = _resolve_reporter(reporter)

    # Phase 2: read attempt identity injected by executor env vars
    correlation_id_str = os.environ.get("DISPATCHIO_CORRELATION_ID")
    correlation_id: UUID | None = None
    if correlation_id_str:
        try:
            correlation_id = UUID(correlation_id_str)
        except ValueError:
            logger.warning("Invalid DISPATCHIO_CORRELATION_ID: %r", correlation_id_str)

    logger.info("Starting job=%s run_key=%s", job_name, resolved_run_key)

    context: dict[str, Any] = {
        "run_key": resolved_run_key,
        "job_name": job_name,
    }

    try:
        _call_fn(fn, context)

        metadata = metadata_fn() if metadata_fn else {}
        logger.info(
            "Job completed successfully: job=%s run_key=%s",
            job_name,
            resolved_run_key,
        )

        if resolved_reporter is not None:
            resolved_reporter.report(
                correlation_id=correlation_id,
                status=Status.DONE,
                metadata=metadata,
            )

    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        logger.error(
            "Job failed: job=%s run_key=%s reason=%s",
            job_name,
            resolved_run_key,
            reason,
        )
        logger.debug("Traceback:", exc_info=True)

        if resolved_reporter is not None:
            resolved_reporter.report(
                correlation_id=correlation_id,
                status=Status.ERROR,
                reason=reason,
            )

        sys.exit(1)

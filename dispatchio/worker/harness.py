"""
run_job() — the Dispatchio job harness.

This is the single entry point job scripts should call. It handles all
the Dispatchio-specific concerns so job code stays pure business logic:

  - Parses --run-id (and --drop-dir for the default filesystem reporter)
    from sys.argv, or accepts them as explicit arguments.
  - Runs the job function inside a try/except.
  - Always posts a completion event (DONE or ERROR) — even if the job
    raises an unexpected exception.

The job function receives only what its signature declares — nothing is
injected unless the function asks for it by name. Available context keys:

    run_id    — the run ID string for this execution  (e.g. "20260414")
    job_name  — the registered job name               (e.g. "ingest")

Usage examples:

    # Pass nothing — function does not need orchestrator context
    def main() -> None:
        ...

    # Pass run_id only
    def main(run_id: str) -> None:
        print(f"Processing {run_id}")
        ...

    # Pass everything via **kwargs
    def main(**ctx) -> None:
        print(ctx["run_id"], ctx["job_name"])
        ...

    if __name__ == "__main__":
        run_job("my_etl_job", main)

The harness reads --run-id and --drop-dir from sys.argv automatically.
Override via explicit arguments or env vars for testing.

Environment variables (all optional):
    DISPATCHIO_RUN_ID      fallback if --run-id not in argv
    DISPATCHIO_DROP_DIR    fallback if --drop-dir not in argv (filesystem reporter)
"""

from __future__ import annotations

import inspect
import logging
import os
import sys
from collections.abc import Callable
from typing import Any

from dispatchio.models import Status
from dispatchio.worker.reporter.base import Reporter
from dispatchio.worker.reporter.filesystem import FilesystemReporter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing helpers
# ---------------------------------------------------------------------------


def _arg_from_argv(flag: str) -> str | None:
    """Extract the value after `flag` in sys.argv, e.g. --run-id 20250115."""
    argv = sys.argv
    try:
        idx = argv.index(flag)
        return argv[idx + 1]
    except (ValueError, IndexError):
        return None


def _resolve_run_id(run_id: str | None) -> str:
    value = run_id or _arg_from_argv("--run-id") or os.environ.get("DISPATCHIO_RUN_ID")
    if not value:
        raise RuntimeError(
            "run_id is required. Pass it explicitly to run_job(), "
            "via --run-id in argv, or via DISPATCHIO_RUN_ID env var."
        )
    return value


def _resolve_reporter(reporter: Reporter | None) -> Reporter | None:
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
        "No reporter configured. Completion events will NOT be sent to Dispatchio. "
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
    run_id: str | None = None,
    reporter: Reporter | None = None,
    metadata_fn: Callable[[], dict[str, Any]] | None = None,
) -> None:
    """
    Execute fn with full Dispatchio lifecycle management.

    fn receives only what its signature declares — see module docstring for
    the available context keys and usage examples.

    Args:
        job_name:           Must match the Job name exactly.
        fn:                 The job function. Declare parameters by name to
                            receive context values (run_id, job_name). Raise
                            any exception to signal failure.
        run_id:             The run_id for this execution. If None, resolved
                            from --run-id argv or DISPATCHIO_RUN_ID env var.
        reporter:           How to send completion events back to Dispatchio.
                            Defaults to FilesystemReporter if --drop-dir or
                            DISPATCHIO_DROP_DIR is set, otherwise a warning is
                            logged and no event is sent.
        metadata_fn:        Optional callable returning a dict of metadata to
                            attach to the DONE event. Called after fn() returns
                            successfully.
    """
    resolved_run_id = _resolve_run_id(run_id)
    resolved_reporter = _resolve_reporter(reporter)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logger.info("Starting job=%s run_id=%s", job_name, resolved_run_id)

    context: dict[str, Any] = {"run_id": resolved_run_id, "job_name": job_name}

    try:
        _call_fn(fn, context)

        metadata = metadata_fn() if metadata_fn else {}
        logger.info(
            "Job completed successfully: job=%s run_id=%s", job_name, resolved_run_id
        )

        if resolved_reporter is not None:
            resolved_reporter.report(
                job_name, resolved_run_id, Status.DONE, metadata=metadata
            )

    except Exception as exc:
        error_reason = f"{type(exc).__name__}: {exc}"
        logger.error(
            "Job failed: job=%s run_id=%s error=%s",
            job_name,
            resolved_run_id,
            error_reason,
        )
        logger.debug("Traceback:", exc_info=True)

        if resolved_reporter is not None:
            resolved_reporter.report(
                job_name,
                resolved_run_id,
                Status.ERROR,
                error_reason=error_reason,
            )

        sys.exit(1)

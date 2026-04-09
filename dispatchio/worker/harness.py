"""
run_job() — the Dispatchio job harness.

This is the single entry point job scripts should call. It handles all
the Dispatchio-specific concerns so job code stays pure business logic:

  - Parses --run-id (and --drop-dir for the default filesystem reporter)
    from sys.argv, or accepts them as explicit arguments.
  - Runs the job function inside a try/except.
  - Always posts a completion event (DONE or ERROR) — even if the job
    raises an unexpected exception.
  - Optionally runs a background heartbeat thread that posts RUNNING
    events at a fixed interval, enabling LOST detection.

Usage (typical job script):

    from dispatchio.worker import run_job

    def main(run_id: str) -> None:
        # pure business logic — no Dispatchio imports needed here
        print(f"Processing {run_id}")
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

import logging
import os
import sys
import threading
from collections.abc import Callable
from typing import Any

from dispatchio.models import Status
from dispatchio.worker.reporter.base import Reporter
from dispatchio.worker.reporter.filesystem import FilesystemReporter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Heartbeat thread
# ---------------------------------------------------------------------------


class _HeartbeatThread(threading.Thread):
    """
    Posts a RUNNING event every `interval` seconds until stopped.
    Runs as a daemon so it never prevents process exit.
    """

    def __init__(
        self,
        job_name: str,
        run_id: str,
        reporter: Reporter,
        interval: int,
    ) -> None:
        super().__init__(daemon=True, name=f"dispatchio-heartbeat-{job_name}")
        self._job_name = job_name
        self._run_id = run_id
        self._reporter = reporter
        self._interval = interval
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.wait(timeout=self._interval):
            try:
                self._reporter.report(self._job_name, self._run_id, Status.RUNNING)
                logger.debug("Heartbeat sent for %s/%s", self._job_name, self._run_id)
            except Exception:
                logger.warning(
                    "Heartbeat failed for %s/%s",
                    self._job_name,
                    self._run_id,
                    exc_info=True,
                )

    def stop(self) -> None:
        self._stop.set()


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

    logger.warning(
        "No reporter configured. Completion events will NOT be sent to Dispatchio. "
        "Pass a reporter to run_job(), use --drop-dir, or set DISPATCHIO_DROP_DIR."
    )
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_job(
    job_name: str,
    fn: Callable[[str], None],
    *,
    run_id: str | None = None,
    reporter: Reporter | None = None,
    heartbeat_interval: int | None = None,
    metadata_fn: Callable[[], dict[str, Any]] | None = None,
) -> None:
    """
    Execute `fn(run_id)` with full Dispatchio lifecycle management.

    Args:
        job_name:           Must match the Job name exactly.
        fn:                 The job function. Receives run_id as its only
                            argument. Raise any exception to signal failure.
        run_id:             The run_id for this execution. If None, resolved
                            from --run-id argv or DISPATCHIO_RUN_ID env var.
        reporter:           How to send completion events back to Dispatchio.
                            Defaults to FilesystemReporter if --drop-dir or
                            DISPATCHIO_DROP_DIR is set, otherwise a warning is
                            logged and no event is sent.
        heartbeat_interval: If set, post a RUNNING event every N seconds in
                            the background. Match this to HeartbeatPolicy on
                            the Job.
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

    # Start heartbeat thread if requested
    heartbeat: _HeartbeatThread | None = None
    if heartbeat_interval is not None and resolved_reporter is not None:
        heartbeat = _HeartbeatThread(
            job_name, resolved_run_id, resolved_reporter, heartbeat_interval
        )
        heartbeat.start()
        logger.debug("Heartbeat thread started (interval=%ds)", heartbeat_interval)

    try:
        fn(resolved_run_id)

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

    finally:
        if heartbeat is not None:
            heartbeat.stop()

"""
Completion reporter factory and helpers.

This module provides a unified interface for jobs to report their completion
status, abstracting away backend-specific details (filesystem vs SQS vs custom).

Typical usage from job code:
    from dispatchio.completion import get_reporter, CompletionReporter

    # Auto-detect reporter from environment (injected by orchestrator)
    reporter = get_reporter(job_name="my_job")
    if reporter:
        reporter.report_success(run_id="20250115", metadata={"rows": 1000})

    # Or manually build for a specific backend
    from dispatchio.config import ReceiverSettings
    receiver_cfg = ReceiverSettings(backend="filesystem", drop_dir="/tmp/drops")
    reporter = build_reporter(receiver_cfg)
    reporter.report_success(run_id="20250115")
"""

from __future__ import annotations

import logging
import os
from typing import Any, Protocol, runtime_checkable

from dispatchio.config.settings import ReceiverSettings
from dispatchio.models import Status
from dispatchio.worker.reporter import Reporter

logger = logging.getLogger(__name__)


@runtime_checkable
class CompletionReporter(Protocol):
    """Higher-level reporter interface for job code."""

    def report_success(
        self, run_id: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Report job completed successfully."""
        ...

    def report_error(self, run_id: str, error_reason: str) -> None:
        """Report job failed with an error."""
        ...

    def report_running(self, run_id: str) -> None:
        """Report job is still running (posts RUNNING status)."""
        ...


class _CompletionReporterAdapter:
    """Adapts low-level Reporter protocol to high-level CompletionReporter interface."""

    def __init__(self, job_name: str, reporter: Reporter | None) -> None:
        self.job_name = job_name
        self.reporter = reporter

    def report_success(
        self, run_id: str, metadata: dict[str, Any] | None = None
    ) -> None:
        if self.reporter is None:
            logger.warning("No reporter configured; completion NOT sent")
            return
        self.reporter.report(self.job_name, run_id, Status.DONE, metadata=metadata)

    def report_error(self, run_id: str, error_reason: str) -> None:
        if self.reporter is None:
            logger.warning("No reporter configured; completion NOT sent")
            return
        self.reporter.report(
            self.job_name, run_id, Status.ERROR, error_reason=error_reason
        )

    def report_running(self, run_id: str) -> None:
        if self.reporter is None:
            logger.warning("No reporter configured; RUNNING status NOT sent")
            return
        self.reporter.report(self.job_name, run_id, Status.RUNNING)


def build_reporter(
    receiver_settings: ReceiverSettings,
) -> Reporter | None:
    """
    Build a Reporter from ReceiverSettings.

    Returns the appropriate reporter for the configured backend
    (filesystem, SQS, etc.), or None if backend is "none".

    Args:
        receiver_settings: ReceiverSettings object (typically from config file)

    Returns:
        Reporter instance, or None if backend="none"

    Raises:
        ImportError: if backend requires an uninstalled optional dependency
        ValueError: if backend is unknown
    """
    if receiver_settings.backend == "none":
        return None

    if receiver_settings.backend == "filesystem":
        from dispatchio.worker.reporter.filesystem import FilesystemReporter

        return FilesystemReporter(receiver_settings.drop_dir)

    if receiver_settings.backend == "sqs":
        try:
            from dispatchio_aws.reporter.sqs import SQSReporter  # type: ignore[import]

            return SQSReporter(
                queue_url=receiver_settings.queue_url,
                region=receiver_settings.region,
            )
        except ImportError:
            raise ImportError(
                "Receiver backend 'sqs' requires dispatchio[aws]. "
                "Install with: pip install dispatchio[aws]"
            )

    raise ValueError(f"Unknown receiver backend: {receiver_settings.backend!r}")


def _reporter_from_env() -> Reporter | None:
    """Create a Reporter using DISPATCHIO_RECEIVER__* environment variables."""
    backend = os.environ.get("DISPATCHIO_RECEIVER__BACKEND", "filesystem").lower()

    if backend == "none":
        return None

    if backend == "filesystem":
        drop_dir = os.environ.get(
            "DISPATCHIO_RECEIVER__DROP_DIR", ".dispatchio/completions"
        )
        settings = ReceiverSettings(backend="filesystem", drop_dir=drop_dir)
        return build_reporter(settings)

    if backend == "sqs":
        queue_url = os.environ.get("DISPATCHIO_RECEIVER__QUEUE_URL")
        region = os.environ.get("DISPATCHIO_RECEIVER__REGION")
        if not queue_url:
            raise ValueError("SQS backend requires DISPATCHIO_RECEIVER__QUEUE_URL")
        settings = ReceiverSettings(
            backend="sqs",
            queue_url=queue_url,
            region=region,
        )
        return build_reporter(settings)

    raise ValueError(f"Unknown DISPATCHIO_RECEIVER__BACKEND: {backend!r}")


def report_external_event(
    event_name: str,
    run_id: str,
    *,
    status: Status = Status.DONE,
    error_reason: str | None = None,
    metadata: dict[str, Any] | None = None,
    receiver_settings: ReceiverSettings | None = None,
) -> bool:
    """
    Publish an external dependency event through the configured receiver backend.

    Args:
        event_name: External dependency key, typically prefixed with "external.".
        run_id: Run identifier used by downstream dependency checks.
        status: Status to report (defaults to DONE).
        error_reason: Optional error message for ERROR events.
        metadata: Optional event metadata.
        receiver_settings: Optional explicit receiver settings. If omitted,
            DISPATCHIO_RECEIVER__* environment variables are used.

    Returns:
        True if an event was published, False when backend is "none".
    """
    reporter = (
        build_reporter(receiver_settings)
        if receiver_settings is not None
        else _reporter_from_env()
    )
    if reporter is None:
        logger.warning("No reporter configured; external event NOT sent")
        return False

    reporter.report(
        event_name,
        run_id,
        status,
        error_reason=error_reason,
        metadata=metadata,
    )
    return True


def report_external_done(
    event_name: str,
    run_id: str,
    *,
    metadata: dict[str, Any] | None = None,
    receiver_settings: ReceiverSettings | None = None,
) -> bool:
    """Convenience helper to publish a DONE event for an external dependency."""
    return report_external_event(
        event_name,
        run_id,
        status=Status.DONE,
        metadata=metadata,
        receiver_settings=receiver_settings,
    )


def get_reporter(job_name: str) -> CompletionReporter:
    """
    Get a CompletionReporter for the current job, auto-configured from environment.

    This is the main entry point for job code. It reads environment variables
    set by orchestrator_from_config() and builds the appropriate reporter.

    If the orchestrator is not configured to use a receiver (backend="none"),
    returns a no-op reporter that logs warnings.

    Args:
        job_name: The name of the job (must match Job.name in orchestrator_from_config)

    Returns:
        CompletionReporter instance (never None — may be no-op if no backend configured)

    Example:
        from dispatchio.completion import get_reporter

        def my_job(run_id: str) -> None:
            print(f"Running {run_id}")
            reporter = get_reporter("my_job")
            try:
                # do work...
                reporter.report_success(run_id, metadata={"status": "ok"})
            except Exception as exc:
                reporter.report_error(run_id, str(exc))
    """
    return _CompletionReporterAdapter(job_name, _reporter_from_env())

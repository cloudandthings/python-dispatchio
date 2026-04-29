"""
Status reporter factory and helpers.

This module provides a unified interface for jobs to report their status,
abstracting away backend-specific details (filesystem vs SQS vs custom).

Typical usage from job code:
    from dispatchio.completion import get_reporter

    # Auto-detect reporter from environment (injected by orchestrator)
    reporter = get_reporter(correlation_id="...")
    if reporter:
        reporter.report_success(metadata={"rows": 1000})

    # Or manually build for a specific backend
    from dispatchio.config import ReceiverSettings
    receiver_cfg = ReceiverSettings(backend="filesystem", drop_dir="/tmp/drops")
    reporter = build_reporter(receiver_cfg)
    reporter.report_success(correlation_id="...")
"""

from __future__ import annotations

import logging
import os
from typing import Any
from uuid import UUID

from dispatchio.config.loader import load_config
from dispatchio.config.settings import ReceiverSettings
from dispatchio.models import Status
from dispatchio.worker.reporter import BaseReporter

logger = logging.getLogger(__name__)


class Reporter:
    """Adapts low-level BaseReporter to high-level Reporter interface."""

    def __init__(
        self, correlation_id: str | UUID, reporter: BaseReporter | None
    ) -> None:
        self.correlation_id = correlation_id
        self.reporter = reporter

    def report_success(self, metadata: dict[str, Any] | None = None) -> None:
        if self.reporter is None:
            logger.warning("No reporter configured; status NOT sent")
            return
        self.reporter.report(self.correlation_id, Status.DONE, metadata=metadata)

    def report_error(self, reason: str) -> None:
        if self.reporter is None:
            logger.warning("No reporter configured; status NOT sent")
            return
        self.reporter.report(self.correlation_id, Status.ERROR, reason=reason)

    def report_running(self) -> None:
        if self.reporter is None:
            logger.warning("No reporter configured; RUNNING status NOT sent")
            return
        self.reporter.report(self.correlation_id, Status.RUNNING)


def build_reporter(
    receiver_settings: ReceiverSettings,
) -> BaseReporter | None:
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

        if not receiver_settings.drop_dir:
            raise ValueError("Filesystem backend requires drop_dir in ReceiverSettings")
        return FilesystemReporter(receiver_settings.drop_dir)

    if receiver_settings.backend == "sqs":
        try:
            from dispatchio_aws.reporter.sqs import SQSReporter  # type: ignore[import]

            if not receiver_settings.queue_url:
                raise ValueError("SQS backend requires queue_url in ReceiverSettings")
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


def _reporter_from_env() -> BaseReporter | None:
    """Create a Reporter from config URI first, then legacy env fallbacks."""
    if os.environ.get("DISPATCHIO_CONFIG_INLINE") or os.environ.get(
        "DISPATCHIO_CONFIG"
    ):
        settings = load_config()
        return build_reporter(settings.receiver)

    backend = os.environ.get("DISPATCHIO_RECEIVER__BACKEND", "none").lower()

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


def get_reporter(correlation_id: str | UUID) -> Reporter:
    """
    Get a Reporter for the current job, auto-configured from environment.

    This is the main entry point for job code. It reads environment variables
    set by orchestrator() and builds the appropriate reporter.

    If the orchestrator is not configured to use a receiver (backend="none"),
    returns a no-op reporter that logs warnings.

    Returns:
        Reporter instance (never None — may be no-op if no backend configured)

    Example:
        from dispatchio.completion import get_reporter

        def my_job(correlation_id: str) -> None:
            print(f"Running {run_key}")
            reporter = get_reporter(correlation_id)
            try:
                # do work...
                reporter.report_success(metadata={"status": "ok"})
            except Exception as exc:
                reporter.report_error(str(exc))
    """
    return Reporter(correlation_id, _reporter_from_env())

"""
Alert layer.

The orchestrator emits AlertEvents; it does not deliver them directly.
An AlertHandler receives events and decides how to route them
(log, SNS, Apprise, PagerDuty, etc.).

This keeps alerting decoupled from the tick engine — swap handlers
without touching orchestration logic.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from dispatchio.models import AlertOn, Attempt

logger = logging.getLogger(__name__)


class AlertEvent(BaseModel):
    """Emitted by the orchestrator when an alert condition is triggered."""

    alert_on: AlertOn
    job_name: str
    run_key: str
    channels: list[str]
    detail: str | None = None
    occurred_at: datetime
    record: Attempt | None = None  # full run record for context


@runtime_checkable
class AlertHandler(Protocol):
    def handle(self, event: AlertEvent) -> None:
        """Deliver or queue an alert. Must not raise — log and swallow errors."""
        ...


class LogAlertHandler:
    """
    Default handler: writes alerts to the Python logging system.
    Replace with an SNS / Apprise handler in production.
    """

    def handle(self, event: AlertEvent) -> None:
        logger.warning(
            "ALERT [%s] job=%s run_key=%s channels=%s detail=%s",
            event.alert_on.value,
            event.job_name,
            event.run_key,
            event.channels,
            event.detail,
        )

"""
Filesystem Reporter.

Writes a StatusEvent JSON file to a drop directory, where the
FilesystemReceiver will pick it up on the next orchestrator tick.

This is the local/dev counterpart to SQSReporter. Use it whenever
you're running jobs as subprocesses and using FilesystemReceiver.

File naming: {job_name}__{run_key}__{status}.json
The filename is informational only — the receiver reads the JSON content.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from dispatchio.models import Status
from dispatchio.receiver.base import StatusEvent
from dispatchio.worker.reporter import BaseReporter

logger = logging.getLogger(__name__)


class FilesystemReporter(BaseReporter):
    def __init__(self, drop_dir: str | Path) -> None:
        self.drop_dir = Path(drop_dir)
        self.drop_dir.mkdir(parents=True, exist_ok=True)

    def report(
        self,
        correlation_id: str | UUID,
        status: Status,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            uuid = (
                UUID(correlation_id)
                if isinstance(correlation_id, str)
                else correlation_id
            )
            event = StatusEvent(
                correlation_id=uuid,
                status=status,
                reason=reason,
                metadata=metadata or {},
            )
            filename = f"{correlation_id}__{status.value}.json"
            path = self.drop_dir / filename
            path.write_text(event.model_dump_json())
            logger.info("Reported %s → %s", correlation_id, status.value)
        except Exception:
            logger.exception(
                "FilesystemReporter failed to write event for %s", correlation_id
            )

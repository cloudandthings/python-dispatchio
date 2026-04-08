"""
Filesystem Reporter.

Writes a CompletionEvent JSON file to a drop directory, where the
FilesystemReceiver will pick it up on the next orchestrator tick.

This is the local/dev counterpart to SQSReporter. Use it whenever
you're running jobs as subprocesses and using FilesystemReceiver.

File naming: {job_name}__{run_id}__{status}.json
The filename is informational only — the receiver reads the JSON content.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from dispatchio.models import Status
from dispatchio.receiver.base import CompletionEvent

logger = logging.getLogger(__name__)


class FilesystemReporter:

    def __init__(self, drop_dir: str | Path) -> None:
        self.drop_dir = Path(drop_dir)
        self.drop_dir.mkdir(parents=True, exist_ok=True)

    def report(
        self,
        job_name: str,
        run_id: str,
        status: Status,
        *,
        error_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            event = CompletionEvent(
                job_name=job_name,
                run_id=run_id,
                status=status,
                error_reason=error_reason,
                metadata=metadata or {},
            )
            filename = f"{job_name}__{run_id}__{status.value}.json"
            path = self.drop_dir / filename
            path.write_text(event.model_dump_json())
            logger.info("Reported %s/%s → %s", job_name, run_id, status.value)
        except Exception:
            logger.exception(
                "FilesystemReporter failed to write event for %s/%s",
                job_name, run_id,
            )

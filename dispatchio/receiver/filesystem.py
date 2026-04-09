"""
Filesystem CompletionReceiver.

Jobs drop a JSON file into a watched directory. drain() picks up all
files, parses them as CompletionEvents, and deletes them.

File naming convention (informational only — content is authoritative):
    {drop_dir}/{job_name}__{run_id}__{status}.json

This is the local-dev / subprocess equivalent of the SQS receiver used
in AWS deployments.

Usage from a job script:
    import json, pathlib
    pathlib.Path("/path/to/completions/myjob__20250115__done.json").write_text(
        json.dumps({"job_name": "myjob", "run_id": "20250115", "status": "done"})
    )

Or use the helper in dispatchio.client:
    from dispatchio.client import signal_done
    signal_done("myjob", "20250115", drop_dir="/path/to/completions")
"""

from __future__ import annotations

import logging
from pathlib import Path

from dispatchio.receiver.base import CompletionEvent

logger = logging.getLogger(__name__)


class FilesystemReceiver:
    def __init__(self, drop_dir: str | Path) -> None:
        self.drop_dir = Path(drop_dir)
        self.drop_dir.mkdir(parents=True, exist_ok=True)

    def drain(self) -> list[CompletionEvent]:
        events: list[CompletionEvent] = []
        for path in sorted(self.drop_dir.glob("*.json")):
            try:
                event = CompletionEvent.model_validate_json(path.read_text())
                events.append(event)
                path.unlink()
            except Exception as exc:
                logger.warning("Skipping malformed completion file %s: %s", path, exc)
        return events

    # ------------------------------------------------------------------
    # Convenience: write a completion event from within a job
    # (used by dispatchio.client helpers and the hello-world example)
    # ------------------------------------------------------------------

    def emit(self, event: CompletionEvent) -> None:
        """Write a completion event file. Called by job code, not the orchestrator."""
        filename = f"{event.job_name}__{event.run_id}__{event.status.value}.json"
        path = self.drop_dir / filename
        path.write_text(event.model_dump_json())

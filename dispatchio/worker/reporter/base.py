"""
Reporter protocol — the job-side mirror of StatusReceiver.

A Reporter knows how to send a StatusEvent from inside a running job
back to the orchestrator's state store.

  Orchestrator side:  StatusReceiver.drain()  ← reads events
  Job side:           Reporter.report()            → writes events

Implementations ship alongside their receiver counterparts:
  FilesystemReporter  ↔  FilesystemReceiver   (core)
  SQSReporter         ↔  SQSReceiver          (dispatchio[aws])
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from dispatchio.models import Status


@runtime_checkable
class BaseReporter(Protocol):
    def report(
        self,
        correlation_id: str | UUID,
        status: Status,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Emit a job event.

        Called by the harness — not directly by job code.
        Must not raise; implementations should log and swallow errors so a
        reporting failure never masks the original job outcome.
        """
        ...

"""
Reporter protocol — the job-side mirror of CompletionReceiver.

A Reporter knows how to send a CompletionEvent from inside a running job
back to the orchestrator's state store.

  Orchestrator side:  CompletionReceiver.drain()  ← reads events
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
class Reporter(Protocol):
    def report(
        self,
        job_name: str,
        run_id: str,
        status: Status,
        *,
        error_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
        logical_run_id: str | None = None,
        attempt: int | None = None,
        dispatchio_attempt_id: UUID | None = None,
    ) -> None:
        """
        Emit a completion event.

        Called by the harness — not directly by job code.
        Must not raise; implementations should log and swallow errors so a
        reporting failure never masks the original job outcome.

        The optional logical_run_id, attempt, and dispatchio_attempt_id fields
        enable Phase 2 strict completion correlation. When provided they are
        included in the CompletionEvent so the orchestrator can match the event
        to the exact AttemptRecord that was submitted.
        """
        ...

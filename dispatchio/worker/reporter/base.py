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
    ) -> None:
        """
        Emit a completion event.

        Called by the harness — not directly by job code.
        Must not raise; implementations should log and swallow errors so a
        reporting failure never masks the original job outcome.
        """
        ...

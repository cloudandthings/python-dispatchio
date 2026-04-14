"""
Tick log — append-only audit trail of orchestrator tick() invocations.

Each tick() call appends one TickLogRecord to the log. This gives support
teams a lightweight history of what happened at each evaluation cycle
without reconstructing it from scattered application logs.

FilesystemTickLogStore writes JSONL (one JSON object per line), which is
easy to tail, parse, and ship to log aggregators.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass
class TickLogRecord:
    """Record of a single tick() invocation."""

    ticked_at: str          # Wall-clock ISO timestamp (UTC) — when tick() actually ran
    reference_time: str     # Logical time passed to tick() (ISO)
    duration_seconds: float
    actions: list[dict]     # Serialised JobTickResult list: job_name, run_id, action, detail


@runtime_checkable
class TickLogStore(Protocol):
    """Append-only store for tick audit records."""

    def append(self, record: TickLogRecord) -> None:
        """Append a tick record to the log."""
        ...

    def list(
        self,
        *,
        limit: int = 50,
        since: str | None = None,
        until: str | None = None,
    ) -> list[TickLogRecord]:
        """Return tick records, most recent first, optionally bounded by ISO time strings."""
        ...


class FilesystemTickLogStore:
    """JSONL-backed tick log stored at a single file path."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def append(self, record: TickLogRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            f.write(json.dumps(asdict(record)) + "\n")

    def list(
        self,
        *,
        limit: int = 50,
        since: str | None = None,
        until: str | None = None,
    ) -> list[TickLogRecord]:
        if not self.path.exists():
            return []
        records: list[TickLogRecord] = []
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    r = TickLogRecord(**data)
                except (json.JSONDecodeError, TypeError):
                    continue
                if since and r.ticked_at < since:
                    continue
                if until and r.ticked_at > until:
                    continue
                records.append(r)
        records.reverse()  # most recent first
        return records[:limit]

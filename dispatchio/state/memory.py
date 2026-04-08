"""
In-memory StateStore.

Intended for unit tests and quick local experimentation.
State is lost when the process exits — do not use in production.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dispatchio.models import RunRecord, Status


class MemoryStateStore:
    """Thread-unsafe in-memory store keyed by (job_name, run_id)."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], RunRecord] = {}

    def get(self, job_name: str, run_id: str) -> RunRecord | None:
        return self._records.get((job_name, run_id))

    def put(self, record: RunRecord) -> None:
        self._records[(record.job_name, record.run_id)] = record.model_copy(deep=True)

    def heartbeat(self, job_name: str, run_id: str, at: datetime | None = None) -> None:
        now = at or datetime.now(tz=timezone.utc)
        existing = self._records.get((job_name, run_id))
        if existing is None:
            self._records[(job_name, run_id)] = RunRecord(
                job_name=job_name,
                run_id=run_id,
                status=Status.RUNNING,
                started_at=now,
                last_heartbeat_at=now,
            )
        else:
            updated = existing.model_copy(
                update={"last_heartbeat_at": now, "status": Status.RUNNING}
            )
            self._records[(job_name, run_id)] = updated

    def list_records(
        self,
        job_name: str | None = None,
        status:   Status | None = None,
    ) -> list[RunRecord]:
        records = self._records.values()
        if job_name is not None:
            records = (r for r in records if r.job_name == job_name)  # type: ignore[assignment]
        if status is not None:
            records = (r for r in records if r.status == status)  # type: ignore[assignment]
        return sorted(records, key=lambda r: (r.job_name, r.run_id))

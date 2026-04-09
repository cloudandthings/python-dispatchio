"""
Filesystem StateStore.

Records are stored as JSON files under:
    {root}/{job_name}/{run_id}.json

Writes are atomic: content is written to a temp file then renamed into place,
so a crash mid-write never leaves a corrupt record.

Layout:
    state/
        ingest/
            20250115.json
            20250116.json
        transform/
            20250115.json
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dispatchio.models import RunRecord, Status


class FilesystemStateStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _path(self, job_name: str, run_id: str) -> Path:
        return self.root / job_name / f"{run_id}.json"

    def _write_atomic(self, path: Path, record: RunRecord) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(record.model_dump_json(indent=2))
            os.replace(tmp, path)  # atomic on POSIX; best-effort on Windows
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ------------------------------------------------------------------
    # StateStore protocol
    # ------------------------------------------------------------------

    def get(self, job_name: str, run_id: str) -> RunRecord | None:
        path = self._path(job_name, run_id)
        if not path.exists():
            return None
        return RunRecord.model_validate_json(path.read_text())

    def put(self, record: RunRecord) -> None:
        self._write_atomic(self._path(record.job_name, record.run_id), record)

    def heartbeat(self, job_name: str, run_id: str, at: datetime | None = None) -> None:
        now = at or datetime.now(tz=timezone.utc)
        existing = self.get(job_name, run_id)
        if existing is None:
            record = RunRecord(
                job_name=job_name,
                run_id=run_id,
                status=Status.RUNNING,
                started_at=now,
                last_heartbeat_at=now,
            )
        else:
            record = existing.model_copy(
                update={"last_heartbeat_at": now, "status": Status.RUNNING}
            )
        self.put(record)

    def list_records(
        self,
        job_name: str | None = None,
        status: Status | None = None,
    ) -> list[RunRecord]:
        records: list[RunRecord] = []

        if job_name is not None:
            job_dirs = [self.root / job_name]
        else:
            job_dirs = [p for p in self.root.iterdir() if p.is_dir()]

        for job_dir in job_dirs:
            if not job_dir.exists():
                continue
            for record_file in sorted(job_dir.glob("*.json")):
                try:
                    record = RunRecord.model_validate_json(record_file.read_text())
                except Exception:
                    continue  # skip corrupt files
                if status is None or record.status == status:
                    records.append(record)

        return sorted(records, key=lambda r: (r.job_name, r.run_id))

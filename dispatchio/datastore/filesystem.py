"""Filesystem-backed DataStore — JSON files, one per key."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dispatchio.datastore.base import DataStore, _resolve_job, _resolve_run_key


class FilesystemDataStore(DataStore):
    """DataStore backed by JSON files under a base directory.

    Directory layout:
        <base_dir>/<namespace>/<job>/<run_key>/<key>.json

    Write is idempotent: writing the same (job, run_key, key) twice overwrites
    the previous value.
    """

    def __init__(self, base_dir: str | Path, namespace: str = "default") -> None:
        super().__init__(namespace=namespace)
        self.base_dir = Path(base_dir)

    def _path(self, job: str, run_key: str, key: str) -> Path:
        safe_key = key.replace("/", "_").replace("\\", "_")
        return self.base_dir / self.namespace / job / run_key / f"{safe_key}.json"

    def write(
        self,
        value: Any,
        *,
        job: str | None = None,
        run_key: str | None = None,
        key: str = "return_value",
    ) -> None:
        p = self._path(_resolve_job(job), _resolve_run_key(run_key), key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(value))

    def read(
        self,
        *,
        job: str,
        run_key: str | None = None,
        key: str = "return_value",
    ) -> Any | None:
        p = self._path(job, _resolve_run_key(run_key), key)
        if not p.exists():
            return None
        return json.loads(p.read_text())

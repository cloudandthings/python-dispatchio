"""In-memory DataStore — for tests and single-process simulations."""

from __future__ import annotations

from typing import Any

from dispatchio.datastore.base import _resolve_job, _resolve_run_key


class MemoryDataStore:
    """DataStore backed by an in-process dict.

    State is not shared across process boundaries.
    Suitable for unit tests and simulations that run in a single process.
    """

    def __init__(self, namespace: str = "default") -> None:
        self.namespace = namespace
        self._store: dict[str, Any] = {}

    def _key(self, job: str, run_key: str, key: str) -> str:
        return f"{self.namespace}/{job}/{run_key}/{key}"

    def write(
        self,
        value: Any,
        *,
        job: str | None = None,
        run_key: str | None = None,
        key: str = "return_value",
    ) -> None:
        self._store[self._key(_resolve_job(job), _resolve_run_key(run_key), key)] = (
            value
        )

    def read(
        self,
        *,
        job: str,
        run_key: str | None = None,
        key: str = "return_value",
    ) -> Any | None:
        return self._store.get(self._key(job, _resolve_run_key(run_key), key))

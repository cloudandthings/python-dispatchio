"""DataStore — protocol and env-var resolution helpers.

DataStore is a key-value store for structured JSON outputs produced by one
job and consumed by another. It is the inter-job data-passing primitive in
dispatchio, analogous to XCom in Airflow.

Key structure: namespace / job / run_id / key

When job or run_id is None in write/read calls, the values are resolved from
environment variables injected by the executor (DISPATCHIO_JOB_NAME,
DISPATCHIO_RUN_ID). This makes the harness API ergonomic:

    store = get_data_store()
    store.write(result)              # job and run_id from env
    store.write(result, key="rows")  # explicit key

    rows = store.read(job="extract", key="rows")
"""

from __future__ import annotations

import os
from typing import Any, Protocol, runtime_checkable


def _resolve_job(job: str | None) -> str:
    if job is not None:
        return job
    val = os.environ.get("DISPATCHIO_JOB_NAME")
    if val:
        return val
    raise ValueError(
        "job is required when DISPATCHIO_JOB_NAME is not set. "
        "Pass job= explicitly or run inside a dispatchio worker."
    )


def _resolve_run_id(run_id: str | None) -> str:
    if run_id is not None:
        return run_id
    val = os.environ.get("DISPATCHIO_RUN_ID")
    if val:
        return val
    raise ValueError(
        "run_id is required when DISPATCHIO_RUN_ID is not set. "
        "Pass run_id= explicitly or run inside a dispatchio worker."
    )


@runtime_checkable
class DataStore(Protocol):
    """Protocol satisfied by any object with write, read, and worker_env.

    The namespace is set at construction — one instance covers one namespace.
    Multiple orchestrators sharing the same backing store should use distinct
    namespaces to avoid key collisions.

    write:      Store a JSON-serialisable value keyed by (job, run_id, key).
    read:       Retrieve a stored value, or None if not found.
    worker_env: Return the env vars that worker subprocesses need to reach
                this store instance. Injected by executors at submit time.
                Returns {} for in-process stores (e.g. MemoryDataStore).
    """

    namespace: str

    def write(
        self,
        value: Any,
        *,
        job: str | None = None,
        run_id: str | None = None,
        key: str = "return_value",
    ) -> None: ...

    def read(
        self,
        *,
        job: str,
        run_id: str | None = None,
        key: str = "return_value",
    ) -> Any | None: ...

    def worker_env(self) -> dict[str, str]: ...

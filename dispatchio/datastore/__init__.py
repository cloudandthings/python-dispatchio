"""dispatchio.datastore — inter-job data passing.

Quick start (orchestrator side):
    from dispatchio.datastore import FilesystemDataStore

    store = FilesystemDataStore(".dispatchio/data", namespace="my-pipeline")
    orchestrator = local_orchestrator(jobs, data_store=store)

Quick start (worker side — inside a job function):
    from dispatchio.datastore import get_data_store

    def discover():
        store = get_data_store()
        store.write(["entity_a", "entity_b"], key="entities")

    def process():
        store = get_data_store()
        entities = store.read(job="discover", key="entities") or []
        for entity in entities:
            ...
"""

import os

from dispatchio.config.loader import load_config
from dispatchio.datastore.base import DataStore
from dispatchio.datastore.memory import MemoryDataStore
from dispatchio.datastore.filesystem import FilesystemDataStore
from dispatchio.datastore.decorators import (
    dispatchio_read_results,
    dispatchio_write_results,
)


def get_data_store(namespace: str | None = None) -> DataStore:
    """Build a DataStore from environment variables.

    Called from worker processes where the executor has injected:
        DISPATCHIO_DATA_DIR       — base directory (FilesystemDataStore)
        DISPATCHIO_DATA_NAMESPACE — namespace (default: "default")

    Args:
        namespace: Override the namespace from env. Rarely needed.

    Raises:
        RuntimeError: If DISPATCHIO_DATA_DIR is not set.
    """
    if os.environ.get("DISPATCHIO_CONFIG_INLINE") or os.environ.get(
        "DISPATCHIO_CONFIG"
    ):
        settings = load_config()
        ds_cfg = settings.data_store
        ns = namespace or ds_cfg.namespace or "default"
        if ds_cfg.backend == "filesystem":
            return FilesystemDataStore(ds_cfg.base_dir, namespace=ns)
        raise RuntimeError(
            f"Unsupported data_store backend in config: {ds_cfg.backend!r}"
        )

    # Legacy fallback
    ns = namespace or os.environ.get("DISPATCHIO_DATA_NAMESPACE", "default")
    data_dir = os.environ.get("DISPATCHIO_DATA_DIR")
    if data_dir:
        return FilesystemDataStore(data_dir, namespace=ns)
    raise RuntimeError(
        "Cannot create DataStore: DISPATCHIO_DATA_DIR is not set. "
        "Set DISPATCHIO_CONFIG_INLINE, DISPATCHIO_CONFIG, or DISPATCHIO_DATA_DIR."
    )


__all__ = [
    "DataStore",
    "FilesystemDataStore",
    "MemoryDataStore",
    "get_data_store",
    "dispatchio_read_results",
    "dispatchio_write_results",
]

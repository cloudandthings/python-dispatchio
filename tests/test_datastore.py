"""Tests for dispatchio.datastore — DataStore protocol and implementations."""

from __future__ import annotations


import pytest

from dispatchio.datastore import (
    DataStore,
    FilesystemDataStore,
    MemoryDataStore,
    dispatchio_read_results,
    dispatchio_write_results,
    get_data_store,
)
from dispatchio.datastore.base import _resolve_job, _resolve_run_key


# ---------------------------------------------------------------------------
# _resolve_job / _resolve_run_key
# ---------------------------------------------------------------------------


class TestResolvers:
    def test_resolve_job_explicit(self):
        assert _resolve_job("my_job") == "my_job"

    def test_resolve_job_from_env(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_JOB_NAME", "env_job")
        assert _resolve_job(None) == "env_job"

    def test_resolve_job_raises_without_env(self, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_JOB_NAME", raising=False)
        with pytest.raises(ValueError, match="job is required"):
            _resolve_job(None)

    def test_resolve_run_key_explicit(self):
        assert _resolve_run_key("20260419") == "20260419"

    def test_resolve_run_key_from_env(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")
        assert _resolve_run_key(None) == "20260419"

    def test_resolve_run_key_raises_without_env(self, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_RUN_KEY", raising=False)
        with pytest.raises(ValueError, match="run_key is required"):
            _resolve_run_key(None)


# ---------------------------------------------------------------------------
# MemoryDataStore
# ---------------------------------------------------------------------------


class TestMemoryDataStore:
    def test_write_and_read(self):
        store = MemoryDataStore()
        store.write(["a", "b"], job="discover", run_key="20260419", key="entities")
        assert store.read(job="discover", run_key="20260419", key="entities") == [
            "a",
            "b",
        ]

    def test_read_missing_key_returns_none(self):
        store = MemoryDataStore()
        assert store.read(job="discover", run_key="20260419", key="missing") is None

    def test_default_key_is_return_value(self):
        store = MemoryDataStore()
        store.write(42, job="j", run_key="r")
        assert store.read(job="j", run_key="r") == 42

    def test_write_is_idempotent(self):
        store = MemoryDataStore()
        store.write("first", job="j", run_key="r", key="k")
        store.write("second", job="j", run_key="r", key="k")
        assert store.read(job="j", run_key="r", key="k") == "second"

    def test_namespace_isolation(self):
        a = MemoryDataStore(namespace="ns_a")
        b = MemoryDataStore(namespace="ns_b")
        a.write("from_a", job="j", run_key="r", key="k")
        assert b.read(job="j", run_key="r", key="k") is None

    def test_json_value_types(self):
        store = MemoryDataStore()
        for value in [None, 1, 3.14, True, "str", [1, 2], {"x": 1}]:
            store.write(value, job="j", run_key="r", key="k")
            assert store.read(job="j", run_key="r", key="k") == value

    def test_write_uses_env_job(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_JOB_NAME", "env_job")
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")
        store = MemoryDataStore()
        store.write("val")
        assert store.read(job="env_job", run_key="20260419") == "val"

    def test_read_uses_env_run_id(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")
        store = MemoryDataStore()
        store.write("val", job="j", run_key="20260419")
        assert store.read(job="j") == "val"

    def test_worker_env_returns_empty(self):
        assert MemoryDataStore().worker_env() == {}

    def test_satisfies_protocol(self):
        assert isinstance(MemoryDataStore(), DataStore)


# ---------------------------------------------------------------------------
# FilesystemDataStore
# ---------------------------------------------------------------------------


class TestFilesystemDataStore:
    def test_write_creates_json_file(self, tmp_path):
        store = FilesystemDataStore(tmp_path)
        store.write(["x", "y"], job="discover", run_key="20260419", key="entities")
        p = tmp_path / "default" / "discover" / "20260419" / "entities.json"
        assert p.exists()

    def test_write_and_read_roundtrip(self, tmp_path):
        store = FilesystemDataStore(tmp_path)
        store.write({"count": 3}, job="j", run_key="r", key="stats")
        assert store.read(job="j", run_key="r", key="stats") == {"count": 3}

    def test_read_missing_returns_none(self, tmp_path):
        store = FilesystemDataStore(tmp_path)
        assert store.read(job="j", run_key="r", key="missing") is None

    def test_default_key_is_return_value(self, tmp_path):
        store = FilesystemDataStore(tmp_path)
        store.write(99, job="j", run_key="r")
        assert store.read(job="j", run_key="r") == 99

    def test_write_is_idempotent(self, tmp_path):
        store = FilesystemDataStore(tmp_path)
        store.write("first", job="j", run_key="r", key="k")
        store.write("second", job="j", run_key="r", key="k")
        assert store.read(job="j", run_key="r", key="k") == "second"

    def test_namespace_in_path(self, tmp_path):
        store = FilesystemDataStore(tmp_path, namespace="my-ns")
        store.write(1, job="j", run_key="r", key="k")
        assert (tmp_path / "my-ns" / "j" / "r" / "k.json").exists()

    def test_namespace_isolation(self, tmp_path):
        a = FilesystemDataStore(tmp_path, namespace="a")
        b = FilesystemDataStore(tmp_path, namespace="b")
        a.write("from_a", job="j", run_key="r", key="k")
        assert b.read(job="j", run_key="r", key="k") is None

    def test_json_value_types(self, tmp_path):
        store = FilesystemDataStore(tmp_path)
        for value in [None, 1, 3.14, True, "str", [1, 2], {"x": 1}]:
            store.write(value, job="j", run_key="r", key="k")
            assert store.read(job="j", run_key="r", key="k") == value

    def test_worker_env_contains_data_dir_and_namespace(self, tmp_path):
        store = FilesystemDataStore(tmp_path, namespace="my-ns")
        env = store.worker_env()
        assert env["DISPATCHIO_DATA_DIR"] == str(tmp_path)
        assert env["DISPATCHIO_DATA_NAMESPACE"] == "my-ns"

    def test_satisfies_protocol(self, tmp_path):
        assert isinstance(FilesystemDataStore(tmp_path), DataStore)

    def test_write_uses_env_job(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_JOB_NAME", "env_job")
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")
        store = FilesystemDataStore(tmp_path)
        store.write("val")
        assert store.read(job="env_job", run_key="20260419") == "val"


# ---------------------------------------------------------------------------
# get_data_store
# ---------------------------------------------------------------------------


class TestGetDataStore:
    def test_returns_filesystem_store_when_dir_set(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        store = get_data_store()
        assert isinstance(store, FilesystemDataStore)

    def test_namespace_from_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("DISPATCHIO_DATA_NAMESPACE", "test-ns")
        store = get_data_store()
        assert store.namespace == "test-ns"

    def test_namespace_arg_overrides_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("DISPATCHIO_DATA_NAMESPACE", "env-ns")
        store = get_data_store(namespace="arg-ns")
        assert store.namespace == "arg-ns"

    def test_default_namespace_is_default(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        monkeypatch.delenv("DISPATCHIO_DATA_NAMESPACE", raising=False)
        store = get_data_store()
        assert store.namespace == "default"

    def test_raises_without_data_dir(self, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_DATA_DIR", raising=False)
        with pytest.raises(RuntimeError, match="DISPATCHIO_DATA_DIR"):
            get_data_store()


# ---------------------------------------------------------------------------
# dispatchio_write_results decorator
# ---------------------------------------------------------------------------


class TestDispatchioWriteResults:
    def test_writes_return_value_with_default_key(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("DISPATCHIO_DATA_NAMESPACE", "ns")
        monkeypatch.setenv("DISPATCHIO_JOB_NAME", "discover")
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")

        @dispatchio_write_results
        def discover() -> list[str]:
            return ["a", "b"]

        out = discover()
        assert out == ["a", "b"]

        store = FilesystemDataStore(tmp_path, namespace="ns")
        assert store.read(job="discover", run_key="20260419", key="return_value") == [
            "a",
            "b",
        ]

    def test_writes_return_value_with_custom_key(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("DISPATCHIO_DATA_NAMESPACE", "ns")
        monkeypatch.setenv("DISPATCHIO_JOB_NAME", "discover")
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")

        @dispatchio_write_results(key="entities")
        def discover() -> list[str]:
            return ["x", "y"]

        discover()

        store = FilesystemDataStore(tmp_path, namespace="ns")
        assert store.read(job="discover", run_key="20260419", key="entities") == [
            "x",
            "y",
        ]

    def test_strict_true_raises_when_store_is_unavailable(self, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_DATA_DIR", raising=False)

        @dispatchio_write_results(strict=True)
        def discover() -> list[str]:
            return ["a"]

        with pytest.raises(RuntimeError, match="DISPATCHIO_DATA_DIR"):
            discover()

    def test_strict_false_does_not_raise_when_store_is_unavailable(self, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_DATA_DIR", raising=False)

        @dispatchio_write_results(strict=False)
        def discover() -> list[str]:
            return ["a"]

        assert discover() == ["a"]


# ---------------------------------------------------------------------------
# dispatchio_read_results decorator
# ---------------------------------------------------------------------------


class TestDispatchioReadResults:
    def test_injects_value_into_target_param(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("DISPATCHIO_DATA_NAMESPACE", "ns")
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")

        store = FilesystemDataStore(tmp_path, namespace="ns")
        store.write(
            ["customer_1", "customer_2"],
            job="discover",
            run_key="20260419",
            key="entities",
        )

        @dispatchio_read_results(param="entities", job="discover", key="entities")
        def process(entities: list[str] | None = None) -> list[str] | None:
            return entities

        assert process() == ["customer_1", "customer_2"]

    def test_does_not_override_explicit_kwarg(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("DISPATCHIO_DATA_NAMESPACE", "ns")
        monkeypatch.setenv("DISPATCHIO_RUN_KEY", "20260419")

        @dispatchio_read_results(param="entities", job="discover", key="entities")
        def process(entities: list[str] | None = None) -> list[str] | None:
            return entities

        assert process(entities=["manual"]) == ["manual"]

    def test_strict_true_raises_when_store_is_unavailable(self, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_DATA_DIR", raising=False)

        @dispatchio_read_results(
            param="entities", job="discover", key="entities", strict=True
        )
        def process(entities: list[str] | None = None) -> list[str] | None:
            return entities

        with pytest.raises(RuntimeError, match="DISPATCHIO_DATA_DIR"):
            process()

    def test_strict_false_injects_none_when_store_is_unavailable(self, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_DATA_DIR", raising=False)

        @dispatchio_read_results(
            param="entities", job="discover", key="entities", strict=False
        )
        def process(entities: list[str] | None = None) -> list[str] | None:
            return entities

        assert process() is None

    def test_raises_when_param_not_in_signature(self):
        with pytest.raises(ValueError, match="cannot be applied"):

            @dispatchio_read_results(param="entities", job="discover")
            def process() -> None:
                return None


# ---------------------------------------------------------------------------
# Orchestrator integration — data_store wired through local_orchestrator
# ---------------------------------------------------------------------------


class TestOrchestratorDataStoreWiring:
    def test_local_orchestrator_stores_data_store(self, tmp_path):
        from dispatchio import local_orchestrator, Job, SubprocessJob

        store = MemoryDataStore()
        orch = local_orchestrator(
            [Job(name="j", executor=SubprocessJob(command=["echo"]))],
            base_dir=tmp_path,
            data_store=store,
        )
        assert orch.data_store is store

    def test_local_orchestrator_no_data_store_is_none(self, tmp_path):
        from dispatchio import local_orchestrator, Job, SubprocessJob

        orch = local_orchestrator(
            [Job(name="j", executor=SubprocessJob(command=["echo"]))],
            base_dir=tmp_path,
        )
        assert orch.data_store is None

    def test_subprocess_executor_receives_data_env(self, tmp_path):
        from dispatchio import local_orchestrator, Job, SubprocessJob
        from dispatchio.executor.subprocess_ import SubprocessExecutor

        state_dir = tmp_path / "state"
        state_dir.mkdir()
        store = FilesystemDataStore(tmp_path / "data", namespace="test")
        orch = local_orchestrator(
            [Job(name="j", executor=SubprocessJob(command=["echo"]))],
            base_dir=state_dir,
            data_store=store,
        )
        sub_exec = orch.executors["subprocess"]
        assert isinstance(sub_exec, SubprocessExecutor)
        assert sub_exec._data_env["DISPATCHIO_DATA_DIR"] == str(tmp_path / "data")
        assert sub_exec._data_env["DISPATCHIO_DATA_NAMESPACE"] == "test"

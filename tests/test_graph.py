"""Tests for dispatchio.graph — JSON graph loading, validation, and orchestrator construction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dispatchio.graph import (
    GraphSpec,
    GraphValidationError,
    dump_schema,
    load_graph,
    orchestrator_from_graph,
    validate_graph,
)
from dispatchio.orchestrator import Orchestrator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw(**overrides) -> dict:
    base: dict = {
        "graph_version": "1",
        "name": "test-pipeline",
        "generated_at": "2026-04-19T10:00:00Z",
        "jobs": [
            {
                "name": "ingest",
                "executor": {
                    "type": "subprocess",
                    "command": ["echo", "{run_key}"],
                },
            }
        ],
    }
    base.update(overrides)
    return base


def _graph_file(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "graph.json"
    p.write_text(json.dumps(data))
    return p


def _daily_cadence() -> dict:
    return {"type": "date", "frequency": "daily", "offset": 0}


def _dep(job_name: str) -> dict:
    return {"kind": "job", "job_name": job_name, "cadence": _daily_cadence()}


# ---------------------------------------------------------------------------
# load_graph — Pydantic-level validation
# ---------------------------------------------------------------------------


class TestLoadGraph:
    def test_valid_minimal_graph(self, tmp_path):
        spec = load_graph(_graph_file(tmp_path, _raw()))
        assert spec.name == "test-pipeline"
        assert spec.graph_version == "1"
        assert len(spec.jobs) == 1
        assert spec.jobs[0].name == "ingest"

    def test_unknown_graph_version_raises(self, tmp_path):
        with pytest.raises(GraphValidationError, match="graph_version"):
            load_graph(_graph_file(tmp_path, _raw(graph_version="99")))

    def test_unknown_top_level_field_raises(self, tmp_path):
        data = _raw()
        data["unknown_field"] = "surprise"
        with pytest.raises(
            GraphValidationError, match="Extra inputs are not permitted"
        ):
            load_graph(_graph_file(tmp_path, data))

    def test_invalid_json_raises(self, tmp_path):
        p = tmp_path / "graph.json"
        p.write_text("not valid json {{{")
        with pytest.raises(GraphValidationError, match="Invalid JSON"):
            load_graph(p)

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_graph(tmp_path / "does_not_exist.json")

    def test_missing_required_field_raises(self, tmp_path):
        data = _raw()
        del data["name"]
        with pytest.raises(GraphValidationError):
            load_graph(_graph_file(tmp_path, data))

    def test_producer_info_loaded(self, tmp_path):
        data = _raw(producer={"name": "my-builder", "version": "1.2.0"})
        spec = load_graph(_graph_file(tmp_path, data))
        assert spec.producer is not None
        assert spec.producer.name == "my-builder"
        assert spec.producer.version == "1.2.0"

    def test_external_dependency_invalid_prefix_raises(self, tmp_path):
        data = _raw(
            external_dependencies=[
                {"name": "no_prefix_here", "cadence": _daily_cadence()}
            ]
        )
        with pytest.raises(GraphValidationError):
            load_graph(_graph_file(tmp_path, data))

    def test_external_dependency_event_prefix_accepted(self, tmp_path):
        data = _raw(
            external_dependencies=[
                {"name": "event.user_registered", "cadence": _daily_cadence()}
            ]
        )
        spec = load_graph(_graph_file(tmp_path, data))
        assert spec.external_dependencies[0].name == "event.user_registered"

    def test_all_errors_reported_at_once(self, tmp_path):
        # Two bad fields — both should appear in the error, not just the first
        data = _raw()
        del data["name"]
        del data["jobs"]
        with pytest.raises(GraphValidationError) as exc_info:
            load_graph(_graph_file(tmp_path, data))
        assert len(exc_info.value.errors) >= 2

    def test_jobs_with_cadence_and_condition(self, tmp_path):
        data = _raw()
        data["jobs"] = [
            {
                "name": "ingest",
                "cadence": _daily_cadence(),
                "condition": {"type": "time_of_day", "after": "06:00:00"},
                "executor": {"type": "subprocess", "command": ["echo"]},
                "retry_policy": {"max_attempts": 3, "retry_on": ["timeout"]},
            }
        ]
        spec = load_graph(_graph_file(tmp_path, data))
        assert spec.jobs[0].retry_policy.max_attempts == 3


# ---------------------------------------------------------------------------
# validate_graph — graph-level invariants
# ---------------------------------------------------------------------------


class TestValidateGraph:
    def test_valid_graph_passes(self):
        spec = GraphSpec.model_validate(_raw())
        validate_graph(spec)  # must not raise

    def test_duplicate_job_names_caught(self):
        data = _raw()
        data["jobs"] = [data["jobs"][0], data["jobs"][0].copy()]
        spec = GraphSpec.model_validate(data)
        with pytest.raises(GraphValidationError) as exc_info:
            validate_graph(spec)
        assert any("Duplicate job name" in e for e in exc_info.value.errors)

    def test_missing_internal_dependency_caught(self):
        data = _raw()
        data["jobs"][0]["depends_on"] = [_dep("nonexistent")]
        spec = GraphSpec.model_validate(data)
        with pytest.raises(GraphValidationError) as exc_info:
            validate_graph(spec)
        assert any("nonexistent" in e for e in exc_info.value.errors)

    def test_undeclared_external_dependency_caught(self):
        data = _raw()
        data["jobs"][0]["depends_on"] = [_dep("event.user_registered")]
        spec = GraphSpec.model_validate(data)
        with pytest.raises(GraphValidationError) as exc_info:
            validate_graph(spec)
        assert any("event.user_registered" in e for e in exc_info.value.errors)

    def test_declared_external_dependency_passes(self):
        data = _raw()
        data["jobs"][0]["depends_on"] = [_dep("event.user_registered")]
        data["external_dependencies"] = [
            {"name": "event.user_registered", "cadence": _daily_cadence()}
        ]
        spec = GraphSpec.model_validate(data)
        validate_graph(spec)  # must not raise

    def test_external_prefix_accepted_for_external_dot(self):
        data = _raw()
        data["jobs"][0]["depends_on"] = [_dep("external.data_feed")]
        data["external_dependencies"] = [
            {"name": "external.data_feed", "cadence": _daily_cadence()}
        ]
        spec = GraphSpec.model_validate(data)
        validate_graph(spec)  # must not raise

    def test_circular_dependency_two_nodes(self):
        data = {
            "graph_version": "1",
            "name": "test",
            "generated_at": "2026-04-19T10:00:00Z",
            "jobs": [
                {
                    "name": "A",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("B")],
                },
                {
                    "name": "B",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("A")],
                },
            ],
        }
        spec = GraphSpec.model_validate(data)
        with pytest.raises(GraphValidationError) as exc_info:
            validate_graph(spec)
        assert any("Circular dependency" in e for e in exc_info.value.errors)

    def test_circular_dependency_three_node_chain(self):
        data = {
            "graph_version": "1",
            "name": "test",
            "generated_at": "2026-04-19T10:00:00Z",
            "jobs": [
                {
                    "name": "A",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("B")],
                },
                {
                    "name": "B",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("C")],
                },
                {
                    "name": "C",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("A")],
                },
            ],
        }
        spec = GraphSpec.model_validate(data)
        with pytest.raises(GraphValidationError) as exc_info:
            validate_graph(spec)
        cycle_errors = [e for e in exc_info.value.errors if "Circular" in e]
        assert cycle_errors

    def test_linear_chain_passes(self):
        data = {
            "graph_version": "1",
            "name": "test",
            "generated_at": "2026-04-19T10:00:00Z",
            "jobs": [
                {"name": "A", "executor": {"type": "subprocess", "command": ["echo"]}},
                {
                    "name": "B",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("A")],
                },
                {
                    "name": "C",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("B")],
                },
            ],
        }
        spec = GraphSpec.model_validate(data)
        validate_graph(spec)  # must not raise

    def test_multiple_errors_collected_before_raise(self):
        data = {
            "graph_version": "1",
            "name": "test",
            "generated_at": "2026-04-19T10:00:00Z",
            "jobs": [
                {
                    "name": "A",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("missing1")],
                },
                {
                    "name": "B",
                    "executor": {"type": "subprocess", "command": ["echo"]},
                    "depends_on": [_dep("missing2")],
                },
            ],
        }
        spec = GraphSpec.model_validate(data)
        with pytest.raises(GraphValidationError) as exc_info:
            validate_graph(spec)
        assert len(exc_info.value.errors) == 2
        assert any("missing1" in e for e in exc_info.value.errors)
        assert any("missing2" in e for e in exc_info.value.errors)


# ---------------------------------------------------------------------------
# dump_schema
# ---------------------------------------------------------------------------


class TestDumpSchema:
    def test_returns_dict(self):
        assert isinstance(dump_schema(), dict)

    def test_is_json_serializable(self):
        json.dumps(dump_schema())  # must not raise

    def test_version_field_constrained_to_literal(self):
        schema_str = json.dumps(dump_schema())
        # The Literal["1"] type must appear in the schema
        assert '"1"' in schema_str

    def test_required_fields_present(self):
        schema = dump_schema()
        # Top-level required fields should appear somewhere in the schema
        schema_str = json.dumps(schema)
        for field in ("graph_version", "name", "generated_at", "jobs"):
            assert field in schema_str


# ---------------------------------------------------------------------------
# orchestrator_from_graph
# ---------------------------------------------------------------------------


class TestOrchestratorFromGraph:
    def test_builds_orchestrator(self):
        spec = GraphSpec.model_validate(_raw())
        orch = orchestrator_from_graph(spec)
        assert isinstance(orch, Orchestrator)

    def test_orchestrator_name_from_spec(self):
        spec = GraphSpec.model_validate(_raw(name="my-etl"))
        orch = orchestrator_from_graph(spec)
        assert orch.namespace == "my-etl"

    def test_strict_false_when_external_deps_declared(self):
        data = _raw()
        data["jobs"][0]["depends_on"] = [_dep("event.user_registered")]
        data["external_dependencies"] = [
            {"name": "event.user_registered", "cadence": _daily_cadence()}
        ]
        spec = GraphSpec.model_validate(data)
        orch = orchestrator_from_graph(spec)
        assert orch.strict_dependencies is False

    def test_strict_true_when_no_external_deps(self):
        spec = GraphSpec.model_validate(_raw())
        orch = orchestrator_from_graph(spec)
        assert orch.strict_dependencies is True

    def test_caller_can_override_strict_dependencies(self):
        spec = GraphSpec.model_validate(_raw())
        orch = orchestrator_from_graph(spec, strict_dependencies=False)
        assert orch.strict_dependencies is False

    def test_raises_graph_validation_error_on_invalid_graph(self):
        data = _raw()
        data["jobs"][0]["depends_on"] = [_dep("missing_job")]
        spec = GraphSpec.model_validate(data)
        with pytest.raises(GraphValidationError):
            orchestrator_from_graph(spec)

    def test_jobs_present_on_orchestrator(self):
        spec = GraphSpec.model_validate(_raw())
        orch = orchestrator_from_graph(spec)
        assert any(j.name == "ingest" for j in orch.jobs)

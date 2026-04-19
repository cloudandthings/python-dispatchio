# JSON Job Graph Decoupling

## Status

Implemented as `dispatchio.graph` (v1).

## Summary

Dispatchio supports two authoring modes:

- Python graph mode (default): users define Job objects directly in Python.
- JSON graph mode (optional): users produce a JSON graph artifact that a runner
  loads and executes.

The goal is to separate control plane concerns (graph creation, governance,
approvals) from runtime concerns (tick evaluation and execution).

## Problem

Most job and dependency definitions are authored in Python (for example, in
jobs.py files). This is flexible, but it couples graph definition to the
runtime process.

Some teams want:

- one process to generate or approve a graph artifact,
- a different process or environment to only execute approved graphs,
- reproducible, versioned orchestration definitions that can be promoted across
  environments.

## Proposed Model

Dispatchio accepts a graph artifact in JSON and converts it to the existing
typed models:

- Job
- Dependency
- Cadence
- Condition
- RetryPolicy
- AlertCondition

Orchestrator behaviour is unchanged. The new capability is a loader and
validation boundary in `dispatchio.graph`.

## Design Goals

- Keep the feature optional.
- Keep Python mode first-class.
- Make graph artifacts versioned and strongly validated.
- Make producer and runner independently deployable.
- Fail fast with clear errors on invalid graph artifacts.

## Non-goals

- Replacing Python mode.
- Allowing arbitrary code execution from JSON.
- Encoding environment-specific infrastructure details inside the graph
  artifact.

## Pros

- Clear control plane and execution plane separation.
- Better auditability and reviewability through artifact diffs.
- Easier environment promotion (same graph artifact in dev/staging/prod).
- Enables non-Python producers to generate dispatchio-compatible graphs.

## Cons

- Less expressiveness than Python for dynamic graph generation.
- New compatibility surface (schema versioning and migrations).
- Additional runtime checks and operational tooling required.
- Producer and runner version skew can cause semantic drift.

---

## Implementation Design

### Two-layer validation

Validation is split into two distinct layers:

**Layer 1 — Pydantic field validation** (happens inside `load_graph`):
Field types, required fields, discriminated unions, and unknown fields are all
checked by Pydantic. `GraphSpec` is configured with `extra="forbid"` so unknown
top-level fields raise immediately rather than being silently ignored.

**Layer 2 — Graph-level invariants** (inside `validate_graph`):
Pydantic cannot check cross-object relationships. `validate_graph` checks:

- Duplicate job names
- Internal dependency references that don't resolve to a job in the graph
- External dependency references (`event.*`, `external.*`) not declared in
  `external_dependencies`
- Circular dependency chains

All errors are collected before raising, so producers can fix everything in one
iteration rather than discovering issues one at a time.

### Version strategy: `Literal` type on `graph_version`

`graph_version` is typed as `Literal["1"]` rather than a free string with a
manual validator. This means:

- Pydantic rejects unknown versions with a clear error naming the valid values.
- The exported JSON Schema shows `"enum": ["1"]` for the version field.
- When v2 is needed, extend to `Literal["1", "2"]` and add a migration path.

### `extra="forbid"` on `GraphSpec`

Unknown top-level fields raise immediately. This prevents silently loading a
malformed artifact and getting confusing runtime errors later. `extra="forbid"`
is applied to `GraphSpec` and its nested envelope models (`ProducerInfo`,
`GraphExternalDependency`), but not to `Job` itself, which is also used
elsewhere.

### Pydantic models as source of truth; JSON Schema via CLI

The JSON Schema for `GraphSpec` is derived directly from the Pydantic models
via `GraphSpec.model_json_schema()`. A separate static schema file is not
maintained — it would drift. Non-Python producers can download the schema at
any time:

```sh
dispatchio graph schema > graph-spec-v1.json
```

---

## Impact Analysis

### API and Data Contract

- `dispatchio/graph/spec.py` — `GraphSpec`, `ProducerInfo`, `GraphExternalDependency`
- `dispatchio/graph/loader.py` — `load_graph`, `validate_graph`, `dump_schema`, `orchestrator_from_graph`
- `GraphValidationError` — collects all errors before raising; has an `errors: list[str]` attribute

### Runtime

- Orchestrator logic stays the same.
- Job validation moves earlier to graph-load time.
- `orchestrator_from_graph` sets `strict_dependencies=False` automatically when
  `external_dependencies` is present.

### Operations and Security

- Reject unknown schema versions with clear errors (Literal type enforcement).
- `extra="forbid"` rejects unknown fields at the envelope level.
- Optional: signature/checksum verification can be added as a pre-load step
  in Phase 2.

### Testing

- Field-level validation (Pydantic): covered by `test_graph.py::TestLoadGraph`
- Graph-level invariants: covered by `test_graph.py::TestValidateGraph`
- Schema contract: covered by `test_graph.py::TestDumpSchema`
- End-to-end: covered by `test_graph.py::TestOrchestratorFromGraph`

---

## Public API

```python
from dispatchio.graph import (
    GraphSpec,               # the envelope model
    ProducerInfo,            # optional producer metadata
    GraphExternalDependency, # external dependency declaration
    GraphValidationError,    # raised by load_graph and validate_graph
    load_graph,              # parse + Pydantic validate a JSON file
    validate_graph,          # graph-level invariants check
    dump_schema,             # returns GraphSpec.model_json_schema()
    orchestrator_from_graph, # full chain: validate → build Orchestrator
)
```

---

## Schema (v1)

Top-level fields:

| Field                  | Type                          | Required |
|------------------------|-------------------------------|----------|
| `graph_version`        | `"1"` (Literal)               | Yes      |
| `orchestrator_name`    | string                        | Yes      |
| `generated_at`         | ISO-8601 datetime             | Yes      |
| `producer`             | ProducerInfo object           | No       |
| `jobs`                 | list of Job objects           | Yes      |
| `external_dependencies`| list of ExternalDependency    | No       |

Each job: all existing `Job` model fields (name, executor, cadence, condition,
depends_on, dependency_mode, dependency_threshold, retry_policy, alerts).

`ProducerInfo`: `name` (string), `version` (string).

`GraphExternalDependency`: `name` (must start with `event.` or `external.`),
`cadence` (Cadence), `description` (string, optional).

---

## Example Artifact

```json
{
  "graph_version": "1",
  "orchestrator_name": "daily-etl",
  "generated_at": "2026-04-19T10:00:00Z",
  "producer": {
    "name": "dispatchio-graph-builder",
    "version": "0.1.0"
  },
  "external_dependencies": [
    {
      "name": "event.user_registered",
      "cadence": {"type": "date", "frequency": "daily", "offset": 0},
      "description": "User registration event from identity system"
    }
  ],
  "jobs": [
    {
      "name": "ingest",
      "cadence": {"type": "date", "frequency": "daily", "offset": 0},
      "executor": {
        "type": "subprocess",
        "command": ["python", "ingest.py", "--run-id", "{run_id}"]
      },
      "retry_policy": {
        "max_attempts": 2,
        "retry_on": ["timeout", "throttle"]
      }
    },
    {
      "name": "transform",
      "executor": {
        "type": "subprocess",
        "command": ["python", "transform.py", "--run-id", "{run_id}"]
      },
      "depends_on": [
        {
          "job_name": "ingest",
          "cadence": {"type": "date", "frequency": "daily", "offset": 0}
        }
      ]
    },
    {
      "name": "send_welcome_email",
      "executor": {
        "type": "python",
        "script": "examples/event_dependencies/my_work.py",
        "function": "send_welcome_email"
      },
      "depends_on": [
        {
          "job_name": "event.user_registered",
          "cadence": {"type": "date", "frequency": "daily", "offset": 0}
        }
      ]
    }
  ]
}
```

---

## Example Flow

### Producer instance

- Builds JSON artifact using the agreed GraphSpec.
- Optionally validates locally: `dispatchio graph validate graph.json`
- Publishes artifact to shared storage (S3, Git, artifact repository).

### Runner instance

```python
from dispatchio.graph import load_graph, orchestrator_from_graph

spec = load_graph("graph.json")           # parse + Pydantic validation
orchestrator = orchestrator_from_graph(   # graph validation + build
    spec, config="dispatchio.toml"
)
orchestrator.tick()
```

`orchestrator_from_graph` calls `validate_graph` internally, so the runner does
not need to call it separately.

---

## CLI

```sh
# Validate a graph artifact (Pydantic + graph-level checks)
dispatchio graph validate graph.json

# Dump the JSON Schema for GraphSpec (for non-Python producers)
dispatchio graph schema
dispatchio graph schema --output graph-spec-v1.json
```

---

## Rollout Plan

### Phase 1: Import only (implemented)

JSON loader, validator, and `orchestrator_from_graph` factory. Python mode
unchanged.

### Phase 2: Export support

Add a utility to emit a `GraphSpec` from an existing Python `list[Job]`. Enables
teams to migrate from Python authoring to JSON artifacts incrementally.

### Phase 3: Producer package/service

Publish a dedicated graph-builder tool with policy checks and optional artifact
signing. Signing support adds a `signature` field to the envelope (verified
before `load_graph` continues).

---

## Compatibility Guidance

- `graph_version` is separate from the dispatchio package version.
- Each runner release documents its supported version range.
- Unknown versions are rejected explicitly (Literal type — no silent
  best-effort parsing).
- `extra="forbid"` on the envelope ensures forward-incompatible fields fail
  loudly on older runners.

---

## Recommendation

Use JSON graph mode for governed, promoted, and multi-environment execution
workflows. Keep Python mode for dynamic and code-centric workflows.

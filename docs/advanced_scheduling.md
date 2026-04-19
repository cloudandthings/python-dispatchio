# Advanced Scheduling Patterns — Design

This document covers eight proposed capability areas.
They are presented as phases because each builds on the previous,
but they are independent enough to be reviewed and accepted separately.

---

## Context: what already works

Before describing gaps, it is worth noting what the existing model
already handles correctly:

- **Monthly and weekly run IDs** — `run_id_expr` already supports `mon0`,
  `week0`, `hour0`, etc. (see `dispatchio/run_id.py`).
- **Cross-granularity dependencies** — because `Dependency.run_id_expr` is
  resolved independently from the parent job's own `run_id_expr`, a daily
  job can already declare `Dependency(job_name="monthly_etl", run_id_expr="mon0")`
  and it will correctly wait for the current month's run to be DONE.
- **Job state is granularity-agnostic** — `RunRecord` stores whatever
  string `run_id` resolves to, so monthly, weekly, and daily records coexist
  in the same state store without any schema changes.

---

## Phase 1 — Extended schedule conditions

This has been implemented.

## Phase 2 — RunID abstraction

This has been implemented.

## Phase 3 — Dynamic job generation (factory callable)

### Problem

Dispatchio's job list was previously fixed at `Orchestrator` construction time.
We have recently added some related behaviour which allows jobs to be added to an orchestrator, although it is not clear if this is done "per tick".

Generating jobs dynamically from metadata (e.g. "process all changed
entities for today") could require a way to re-derive the job list on each tick. Unless there is another pattern / approach that should be used that perhaps works within the existing related behaviour.

### Related behaviour

Dynamic job graphs are now supported via explicit mutation APIs on
`Orchestrator`:

- `add_job(job)` / `add_jobs(jobs)`
- `remove_job(job_name)`
- duplicate names raise `ValueError`
- dependency validation is re-run before `tick()` if and when the job graph changed.

Two constructor flags control strictness:

- `strict_dependencies` (default `True`): unresolved dependencies raise when strict mode is enabled, or if False then warn (cross-orchestrator friendly)
- `allow_runtime_mutation` (default `False`): if enabled, jobs can be added or
    removed after ticks have already run.

This enables an orchestrator-first workflow from config:

```python
orchestrator = orchestrator_from_config(
        config="dispatchio.toml",
        allow_runtime_mutation=True,
)

orchestrator.add_jobs(initial_jobs)

# Later, after some ticks already ran:
orchestrator.add_job(dynamic_job)
```

`orchestrator_from_config` now also accepts no initial jobs, making this flow
natural for pipelines that discover work at runtime.

### Alternative approach (factory callable)

Callable job factories remain a valid future extension when teams want to
regenerate the full graph each tick from artifacts/metadata. With the current
APIs, many of those use cases can be handled by mutating the graph explicitly
instead of replacing it wholesale.

### Behaviour details

| Concern | Behaviour |
|---|---|
| Unresolved dependency warnings | Validated before `tick()` when the graph changed; warning or error depending on `strict_dependencies` |
| Jobs that disappear between ticks | Their existing `RunRecord` in the state store is preserved; they are simply not evaluated that tick |
| State store | Unchanged — dynamic jobs write the same `RunRecord` format as static jobs |
| `orchestrator_from_config` | Can now create an empty orchestrator (`jobs` omitted) for orchestrator-first registration |
| Duplicate names | Rejected immediately with `ValueError` |

### Simple fan-in example (factory alternative)

```python
def job_factory(reference_time: datetime, artifacts: ArtifactStore) -> list[Job]:
    run_id = reference_time.strftime("%Y%m%d")

    # Read which entities changed today (see Phase 4 for ArtifactStore)
    changed = artifacts.read(
        job="discover_entities",
        run_id=run_id,
        key="entities",
    ) or []

    entity_jobs = [
        Job.create(f"process_{entity}", SubprocessJob(command=[..., entity]))
        for entity in changed
    ]

    collector = Job(
        name="collect",
        depends_on=entity_jobs,
        executor=...,
    )

    return [*entity_jobs, collector]

orchestrator = Orchestrator(jobs=job_factory, artifact_store=FilesystemArtifactStore(...), ...)
```

### Scope of change

- `dispatchio/orchestrator.py` — mutable APIs (`add_job`, `add_jobs`, `remove_job`), duplicate checks, pre-tick graph refresh
- `dispatchio/config/loader.py` — `orchestrator_from_config` now allows empty initial jobs
- `tests/test_orchestrator.py` — mutation lifecycle, strict dependencies, duplicate-name tests
- `tests/test_config.py` — empty-jobs factory test
- `examples/` — new example directory `dynamic_registration`

---

## Phase 4 — DataStore

### Status

Implemented as `dispatchio.datastore`.

### Problem

Jobs in the same pipeline often need to pass structured data to each other —
for example, a discovery job that writes a list of changed entities so a
downstream job knows what to process.

`StateStore` is the wrong place for this: it stores execution lifecycle records
(attempts, statuses, timestamps), not job output data.

`DataStore` is the answer: a simple key-value store for JSON-serialisable
values, keyed by `(namespace, job, run_id, key)`.

### Naming

`MetadataStore` was rejected — "metadata" means data *about* jobs, which is
already `StateStore`'s domain. `ArtifactStore` was considered but implies binary
build artifacts. `DataStore` is neutral and accurate.

### Key structure

```text
<namespace> / <producer_job> / <run_id> / <key>
```

- `namespace` is set at construction (default `"default"`). Multiple
  orchestrators sharing a backing store use distinct namespaces.
- `key` defaults to `"return_value"`.
- When `job` or `run_id` is `None` in a write/read call, the values are
  resolved from `DISPATCHIO_JOB_NAME` and `DISPATCHIO_RUN_ID` env vars
  injected by the executor. This makes the harness API ergonomic:

```python
store = get_data_store()
store.write(entities, key="entities")   # job and run_id from env
```

### Protocol

```python
class DataStore(Protocol):
    namespace: str

    def write(self, value, *, job=None, run_id=None, key="return_value") -> None: ...
    def read(self, *, job, run_id=None, key="return_value") -> Any | None: ...
    def worker_env(self) -> dict[str, str]: ...
```

`worker_env()` returns the env vars that worker subprocesses need to access
this store instance. Executors call it at submit time to inject config without
requiring isinstance checks in the factories:

- `MemoryDataStore.worker_env()` → `{}` (in-process only)
- `FilesystemDataStore.worker_env()` → `{"DISPATCHIO_DATA_DIR": ..., "DISPATCHIO_DATA_NAMESPACE": ...}`

### Harness integration

Workers call `get_data_store()` — no manual configuration needed:

```python
from dispatchio.datastore import get_data_store

def discover():
    store = get_data_store()
    entities = query_changed_entities()
    store.write(entities, key="entities")   # job + run_id from env

def process():
    store = get_data_store()
    entities = store.read(job="discover", key="entities") or []
    for entity in entities:
        ...
```

The orchestrator injects `DISPATCHIO_JOB_NAME`, `DISPATCHIO_DATA_DIR`, and
`DISPATCHIO_DATA_NAMESPACE` into every worker subprocess automatically.

### Configuration (dispatchio.toml)

```toml
[dispatchio.data_store]
backend   = "filesystem"
base_dir  = ".dispatchio/data"
namespace = "my-pipeline"
```

Or in Python:

```python
from dispatchio import local_orchestrator
from dispatchio.datastore import FilesystemDataStore

store = FilesystemDataStore(".dispatchio/data", namespace="my-pipeline")
orchestrator = local_orchestrator(jobs, data_store=store)
```

### Backends (v1)

| Class | Use case |
|---|---|
| `MemoryDataStore` | Tests, single-process simulations |
| `FilesystemDataStore` | Local dev and simple single-node deployments |

Deferred: `SQLiteDataStore` (batched writes, better for fan-out), DynamoDB
backend (distributed deployments).

### Deferred

- `DataStoreCache` — load/flush optimisation for fan-out patterns with many
  dynamic child jobs. Not needed for the primary use case.
- Escape hatch (`get/put/delete/list_keys`) — direct key access for custom
  naming schemes. Deferred until a concrete use case emerges.
- Factory callable integration — `Orchestrator(job_factory=...)` receiving
  the DataStore as an injected argument.

### Scope of change (implemented)

- `dispatchio/datastore/base.py` — `DataStore` protocol, `_resolve_job`, `_resolve_run_id`
- `dispatchio/datastore/memory.py` — `MemoryDataStore`
- `dispatchio/datastore/filesystem.py` — `FilesystemDataStore`
- `dispatchio/datastore/__init__.py` — `get_data_store()` factory
- `dispatchio/executor/python_.py` + `subprocess_.py` — `data_env` param; inject `DISPATCHIO_JOB_NAME`, `DISPATCHIO_DATA_DIR`, `DISPATCHIO_DATA_NAMESPACE`
- `dispatchio/orchestrator.py` — `data_store` param (stored for future factory use)
- `dispatchio/config/settings.py` — `DataStoreSettings` + `[dispatchio.data_store]` section
- `dispatchio/config/loader.py` — `_build_data_store`, relative path resolution for `base_dir`
- `dispatchio/__init__.py` — re-export `DataStore`, `FilesystemDataStore`, `MemoryDataStore`, `get_data_store`; `local_orchestrator` accepts `data_store`
- `examples/data_store/` — discover → DataStore → process example

---

## Phase 5 — Cascading skip on permanent failure

### Problem

When an upstream job exhausts all retries and stays in ERROR (or LOST),
every downstream job that depends on it waits forever — `_unmet_dependencies`
never returns empty because `required_status=DONE` can never be satisfied.
This is a silent, operational dead-end that becomes more likely with fan-in
patterns containing many dynamic jobs.

### Proposed change

Extend `_evaluate_job` with a "blocked forever" check. After the normal
unmet-dependency scan, inspect whether any unmet dependency is both:

- in a **terminal** state (`ERROR`, `LOST`, or `SKIPPED`), and
- **not** satisfying `required_status`

If so, the dependent job can never proceed — transition it to `SKIPPED`
and log a warning naming the blocking upstream.

```python
# In _evaluate_job, after the existing unmet-dependency block:

permanently_blocked = [
    dep for dep in unmet
    if (rec := self.state.get(dep.job_name, resolve_run_id(dep.run_id_expr, reference_time)))
    and rec.is_terminal()
    and rec.status != dep.required_status
]
if permanently_blocked:
    detail = ", ".join(
        f"{d.job_name} is {self.state.get(d.job_name, ...).status.value}"
        for d in permanently_blocked
    )
    skipped_record = RunRecord(job_name=job.name, run_id=run_id,
                               status=Status.SKIPPED, ...)
    self.state.put(skipped_record)
    return JobTickResult(job_name=job.name, run_id=run_id,
                         action=JobAction.SKIPPED_UPSTREAM_FAILED, detail=detail)
```

This cascades naturally: once a downstream is SKIPPED, any jobs depending
on *it* are blocked by a terminal SKIPPED record and will be SKIPPED on the
next tick.

### New `JobAction` value

`SKIPPED_UPSTREAM_FAILED` — distinguishes "waiting on dependency" from
"will never run because upstream is terminal".

### Scope of change

- `dispatchio/models.py` — add `SKIPPED_UPSTREAM_FAILED` to `JobAction`
- `dispatchio/orchestrator.py` — extend `_evaluate_job`
- `tests/test_orchestrator.py` — cascade tests: direct upstream failure,
  multi-hop cascade, cascade with retries still in-flight

---

## Phase 6 — Dependency satisfaction modes

This has been implemented.

## Phase 7 — Backfill / date-range replay

### Problem

`simulate()` supports a fixed `reference_time`, making one-off historical
runs possible. But reprocessing a range of historical dates (e.g. re-running
six months of monthly jobs after a pipeline fix) requires a loop that the
user has to write every time.

### Proposed change

Add a `backfill()` function to `dispatchio/simulate.py`:

```python
def backfill(
    orchestrator: Orchestrator,
    start: date | datetime,
    end:   date | datetime,
    *,
    granularity:         str      = "day",      # "day", "week", "mon", "hour"
    tick_interval:       float    = 0.0,        # 0 = no sleep (historical)
    max_ticks_per_date:  int      = 20,
    stop_when:           Callable | None = None,
) -> None:
```

`backfill` iterates from `start` to `end` inclusive, stepping by `granularity`,
and calls `simulate()` for each step with the appropriate `reference_time`.

The `tick_interval=0` default is intentional: historical runs should not
sleep between ticks. The caller can pass a non-zero value if the jobs
submit work to a rate-limited external system.

Backfill is **idempotent** by design — jobs that are already DONE for a
given date are silently skipped, so re-running is safe.

### Example

```python
from datetime import date
from dispatchio import backfill

# Re-run all monthly jobs from Jan–Jun 2025
backfill(orchestrator, start=date(2025, 1, 1), end=date(2025, 6, 1),
         granularity="mon")
```

### Scope of change

- `dispatchio/simulate.py` — add `backfill()`, add date-stepping helper
- `dispatchio/__init__.py` — re-export `backfill`
- `tests/test_simulate.py` (new) — date iteration, idempotency, granularity steps
- `mise-tasks/backfill` — optional task wrapper (same pattern as `build-cookbook`)

---

## Advanced — Hierarchical fan-in (open design question)

### The scenario

> X = daily summary jobs (one run per day, `run_id_expr="day0"`)
> Y = weekly summary job (one run per week, `run_id_expr="week0"`)
> Y must wait for all X jobs in its week to complete.

This exposes a "two dates" problem: `Dependency.run_id_expr` offsets
are relative to `reference_time` (today), but "all X jobs in this week"
requires knowing *which days belong to the current week* at evaluation time.

### Option A — Factory computes offsets (recommended for now)

The factory receives `reference_time`, determines which days of the
current week have passed, and builds `depends_on` with explicit `dayN`
offsets. The complexity lives in user code.

```python
def factory(reference_time: datetime) -> list[Job]:
    today = reference_time.date()
    monday = today - timedelta(days=today.weekday())

    # Daily X jobs: one per day Mon–today
    x_jobs = [
        Job(name="daily_x", cadence=DAILY, ...)
    ]
    # NOTE: daily_x is a single job definition; state is keyed by (name, run_id).
    # Each day it runs once and is DONE for that day's run_id.

    # Weekly Y — depends on daily_x for each day Mon–today
    days_elapsed = (today - monday).days + 1
    weekly_deps = [
        Dependency(
            job_name="daily_x",
            cadence=DateCadence(frequency=Frequency.DAILY, offset=-i),
        )
        for i in range(days_elapsed)       # offset 0=today, -1=yesterday, …
    ]
    y_job = Job(
        name="weekly_y",
        cadence=WEEKLY,
        depends_on=weekly_deps,
        executor=...,
    )
    return [*x_jobs, y_job]
```

**Limitation:** on Monday, `weekly_y` will not wait for Tue–Sun
(they haven't happened yet). This is actually correct behaviour for
an incremental weekly job — it runs as soon as it can, which is when
*all days up to today* are done. If it should wait for the full week
before running, add `TimeCondition(after_day=...)` or a fixed weekly
trigger schedule.

### Option B — New `WeekDayCadence` type (future enhancement)

A new `WeekDayCadence(day=0)` (Monday of current week), `WeekDayCadence(day=1)`
(Tuesday), etc. would let the orchestrator itself resolve the mapping.
This avoids factory complexity for a common pattern but adds a new
member to the `Cadence` union.

**Deferred** — Option A is sufficient for now.

### Option C — Separate `logical_run_id` from execution `run_id` (future enhancement)

Some workflows have a *business date* (the period the job covers) that
is distinct from the *execution date* (when it runs). For example, a
reprocessing job for last Monday runs today but its outputs should be
keyed to last Monday's run_id.

`RunRecord` and `Job` could gain an optional `logical_run_id_expr`
field. State would be stored under `logical_run_id` but evaluated under
the execution `run_id`.

**Deferred** — this is a significant model change. The factory pattern
(Option A) handles most reprocessing needs without it.

---

## Implementation sequence

| Phase | Scope | Prerequisite |
|---|---|---|
| 1 — Extended schedule conditions | `Condition` Protocol + concrete types, orchestrator, tests, 1 example | None |
| 2 — RunID abstraction | `Frequency`, `DateCadence`, `Cadence`, constants, `cadence` field, tests | None (independent of Phase 1) |
| 3 — Factory callable | `Orchestrator` factory support, tests, 1 example | Phase 2 (jobs carry `cadence`) |
| 4 — Artifact store | New `dispatchio/artifact/` package, `ArtifactStoreCache`, namespace support, harness injection, 1 example | Phase 3 (factory reads from it) |
| 5 — Cascading skip | Orchestrator change, tests | None |
| 6 — Dependency modes | Model change, orchestrator, tests | Phase 5 (replaces it) |
| 7 — Backfill helper | `simulate.py` extension, tests | Phase 1 (date stepping) |
| Advanced hierarchical | Design iteration needed | Phases 3 + 4 |

Phases 1, 2, and 5 are independent and can proceed in parallel.
Phase 3 depends on Phase 2 so that generated `Job` objects
carry a typed `Cadence` from the start.
Phase 4 depends on Phase 3 in that its primary consumer is the factory pattern.

---

## Open questions

1. **`TimeCondition.after` becoming optional** — is there existing
   config in the wild that relies on `after` being required? If so,
   a validator requiring at least one field to be set is the safe path.

2. **Factory caching** — should the orchestrator cache the factory's
   return value within a tick, or call it once and use that list for
   the whole tick? (The latter is assumed above and is strongly preferred
   for consistency within a tick.)

3. **Empty factory return** — if the factory returns `[]` (e.g. because
   the discovery job hasn't written metadata yet), the tick is a no-op.
   This is correct behaviour, but should it log a warning?

4. **Artifact store config** — should the artifact store be configured
   under the same `dispatchio.toml` as the state store, or separately?
   Sharing the same section (with sub-keys) seems cleaner.

5. **`ArtifactStoreCache` flush on error** — if an exception occurs mid-tick
   after some writes have been buffered, should `flush()` still be called
   (preserving partial results) or skipped (keeping the store clean)?
   The safer default is to flush — partial artifacts are preferable to lost
   artifacts because `write` is idempotent for a given (namespace, job, run_id, key).

6. **SQLite + S3 sync protocol** — copy-on-open / upload-on-close works for
   single-orchestrator deployments. If two orchestrators share a SQLite file
   via S3, a last-writer-wins race is possible. This should be called out
   clearly in docs; the DynamoDB backend is the answer when true concurrency
   is required.

7. **Hierarchical option B (week_day expressions)** — worth a follow-up
   design pass once the factory pattern is in use and we can see which
   mappings are written repeatedly.

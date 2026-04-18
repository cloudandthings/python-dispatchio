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

## Phase 4 — Artifact store

### Problem

Dynamic job factories need to read structured data that was written by a
previous job (e.g. a discovery job that determines which entities changed).
Currently there is no Dispatchio-native place to put this data.

The name `MetadataStore` was considered and rejected — "metadata" in most
systems means data *about* jobs (status, timestamps), which is already
`StateStore`'s domain. What factories need is a store for named, structured
outputs produced by one job and consumed by another — the standard term in
data and CI/CD pipelines is *artifact*. `ArtifactStore` with `write`/`read`
method names is the clearest expression of this without implying a specific
value type or file-based storage.

### Namespacing

An `ArtifactStore` is constructed with a `namespace` (default `"default"`).
Multiple top-level `Orchestrator` instances that share the same backing
store can use distinct namespaces so that their artifact keys never collide:

```python
orchestrator_a = Orchestrator(jobs=..., artifact_store=FilesystemArtifactStore(
    path=ARTIFACT_DIR, namespace="pipeline_a"
))
orchestrator_b = Orchestrator(jobs=..., artifact_store=FilesystemArtifactStore(
    path=ARTIFACT_DIR, namespace="pipeline_b"
))
```

The full internal key structure is `<namespace>/<producer_job>/<run_id>/<key>`.
A factory reading artifacts never needs to know the namespace — it comes
from the store instance injected into the orchestrator.

### Proposed interface

```python
class ArtifactStore(Protocol):
    namespace: str  # set at construction; default "default"

    # Opinionated API: identify values by producer job + logical key,
    # scoped by run_id.
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

    # Escape hatch: direct key/value access for custom naming schemes.
    # Keys are always interpreted within the store's namespace.
    def get(self, key: str) -> Any | None: ...
    def put(self, key: str, value: Any) -> None: ...
    def delete(self, key: str) -> None: ...
    def list_keys(self, prefix: str = "") -> list[str]: ...
```

Values are any JSON-serialisable Python object.
Dispatchio's default key strategy (inspired by Airflow XCom lookup semantics)
is:

- `namespace` + `producer_job` + `run_id` + `key`
- default `key` is `"return_value"`
- default `run_id` in worker context comes from `DISPATCHIO_RUN_ID`
- default `namespace` in worker context comes from `DISPATCHIO_ARTIFACT_NAMESPACE`

By default this resolves to a full key like
`"default/discover_entities/20260115/entities"`.

Users who want a different convention can either:

- pass explicit `key` to `get/put/delete` (it is still scoped to the namespace), or
- configure a custom key strategy callable in artifact settings.

### Tick-level I/O caching

The artifact store is read at tick start (by the job factory) and written
at tick end (by completion events received from workers). A naive
implementation that issues a network or filesystem call for every individual
`write`/`read` will become a bottleneck in fan-out scenarios with many
dynamic child jobs.

The preferred pattern is a **load/flush cycle** tied to the tick boundary:

```python
class ArtifactStoreCache(ArtifactStore):
    """
    Wraps any ArtifactStore. Loads the full namespace into memory at
    construction (or on load()), buffers all writes, and flushes on flush()
    or __exit__. Individual write/read calls touch only the in-memory dict.
    """
    def __init__(self, store: ArtifactStore) -> None: ...
    def load(self) -> None: ...   # reads namespace from backing store once
    def flush(self) -> None: ...  # writes buffered changes back in one batch
    def __enter__(self) -> "ArtifactStoreCache": ...
    def __exit__(self, *_: Any) -> None: ...  # calls flush()
```

`Orchestrator` accepts an optional `ArtifactStoreCache` and, when present,
calls `load()` before the factory callable and `flush()` after phase 5
(submissions). Implementations that do not use the cache can be passed
directly; the load/flush hooks become no-ops.

The factory callable receives the orchestrator's artifact store directly
(as an injected argument), so user code never manages the cache lifecycle
manually:

```python
def job_factory(reference_time: datetime, artifacts: ArtifactStore) -> list[Job]:
    entities = artifacts.read(job="discover_entities", run_id=run_id, key="entities") or []
    ...
```

The updated factory type alias is:

```python
JobFactory = Callable[[datetime, ArtifactStore], list[Job]]
```

### Backend recommendations

| Class | Use case |
|---|---|
| `MemoryArtifactStore` | Tests, in-process demos |
| `FilesystemArtifactStore` | Local dev (JSON files, one file per key) |
| `SQLiteArtifactStore` | Production single-node and S3-synced deployments |
| `DynamoDBArtifactStore` | Distributed / multi-node deployments (future) |

**SQLite is the preferred production backend** for the load/flush pattern.
Loading an entire namespace is a single `SELECT WHERE namespace = ?`; flushing
is a single `BEGIN … COMMIT` transaction. This compares very favourably to
DynamoDB, where a full-namespace scan is expensive and each write is an
individual `PutItem` call.

For serverless / ephemeral orchestrators (Lambda, ECS task), the natural
pattern is: copy the SQLite file from S3 at tick start, load into
`ArtifactStoreCache`, run the tick, flush, upload back to S3. This keeps
I/O outside the hot path and keeps latency predictable.

DynamoDB remains appropriate when multiple orchestrator instances run
concurrently and write payloads independently (true horizontal scaling);
at that point the load/flush cycle is no longer safe without additional
locking, and per-item access is the right model.

### Harness integration

For a job to write payloads it needs a reference to the store.
The cleanest approach is to inject the store path/config via env vars
(`DISPATCHIO_ARTIFACT_DIR` and `DISPATCHIO_ARTIFACT_NAMESPACE` for filesystem),
analogous to how `DISPATCHIO_DROP_DIR` is injected today. The worker then
imports `get_artifact_store` from dispatchio and writes directly:

```python
# Inside a discovery job worker function
from dispatchio.artifact import get_artifact_store

def discover(run_id: str) -> None:
    store = get_artifact_store()   # reads DISPATCHIO_ARTIFACT_DIR / NAMESPACE from env
    entities = query_database_for_changes(run_id)
    # Full key: <namespace>/discover_entities/<run_id>/entities
    store.write(entities, job="discover_entities", run_id=run_id, key="entities")

def build_jobs(run_id: str) -> list[str]:
    store = get_artifact_store()
    return store.read(job="discover_entities", run_id=run_id, key="entities") or []
```

### Scope of change

- `dispatchio/artifact/base.py` — `ArtifactStore` protocol + `ArtifactRecord` model + `ArtifactStoreCache`
- `dispatchio/artifact/memory.py` — `MemoryArtifactStore`
- `dispatchio/artifact/filesystem.py` — `FilesystemArtifactStore`
- `dispatchio/artifact/sqlite_.py` — `SQLiteArtifactStore`
- `dispatchio/artifact/__init__.py` — `get_artifact_store()` factory (reads env) + key strategy wiring
- `dispatchio/orchestrator.py` — accept `artifact_store` arg; inject into `JobFactory`; call `load()`/`flush()` around tick when `ArtifactStoreCache` is provided
- `dispatchio/executor/python_.py` + `subprocess_.py` — inject `DISPATCHIO_ARTIFACT_DIR` + `DISPATCHIO_ARTIFACT_NAMESPACE`
- `dispatchio/config/settings.py` — optional `[dispatchio.artifact]` config section
- `dispatchio/__init__.py` — re-export `ArtifactStore`, `ArtifactStoreCache`

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

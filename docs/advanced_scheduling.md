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

Dispatchio's job list is currently fixed at `Orchestrator` construction time.
Generating jobs dynamically from metadata (e.g. "process all changed
entities for today") requires a way to re-derive the job list on each tick.

### Proposed change

Allow `jobs` to be a callable as well as a plain list.

```python
# Type alias (illustrative)
JobFactory = Callable[[datetime], list[Job]]

class Orchestrator:
    def __init__(
        self,
        jobs: list[Job] | JobFactory,
        ...
    )
```

At the start of each tick, if `jobs` is callable, invoke it with
`reference_time` to obtain the current job list. The `_job_index` is
rebuilt from this list before evaluation begins.

The factory receives `reference_time` so it can scope its queries to
the correct logical period.

### Behaviour details

| Concern | Behaviour |
|---|---|
| Unresolved dependency warnings | Moved from `__init__` to the start of each tick when a factory is in use |
| Jobs that disappear between ticks | Their existing `RunRecord` in the state store is preserved; they are simply not evaluated that tick |
| State store | Unchanged — dynamic jobs write the same `RunRecord` format as static jobs |
| `orchestrator_from_config` | Not affected — factories bypass config and are passed directly to `Orchestrator` |
| `_warn_unresolved_dependencies` | Still fires; for factories, fires per-tick |

### Simple fan-in example

```python
def job_factory(reference_time: datetime) -> list[Job]:
    run_id = reference_time.strftime("%Y%m%d")

    # Read which entities changed today (see Phase 4 for MetadataStore)
    changed = metadata_store.get(
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

orchestrator = Orchestrator(jobs=job_factory, ...)
```

### Scope of change

- `dispatchio/orchestrator.py` — accept callable `jobs`; rebuild `_job_index` per tick when factory is in use
- `tests/test_orchestrator.py` — factory tests
- `examples/` — new example directory `dynamic_jobs`

---

## Phase 4 — Metadata store

### Problem

Dynamic job factories need to read metadata that was written by a
previous job (e.g. a discovery job that determines which entities
changed). Currently there is no Dispatchio-native place to put this data.

### Proposed interface

```python
class MetadataStore(Protocol):
    # Opinionated API (XCom-like): identify values by producer + logical key,
    # scoped by run_id.
    def push(
        self,
        value: Any,
        *,
        job: str | None = None,
        run_id: str | None = None,
        key: str = "return_value",
    ) -> None: ...

    def pull(
        self,
        *,
        job: str,
        run_id: str | None = None,
        key: str = "return_value",
    ) -> Any | None: ...

    # Escape hatch: direct key/value access for custom naming schemes.
    def get(self, full_key: str) -> Any | None: ...
    def put(self, full_key: str, value: Any) -> None: ...
    def delete(self, full_key: str) -> None: ...
    def list_keys(self, prefix: str = "") -> list[str]: ...
```

Values are any JSON-serialisable Python object.
Dispatchio's default key strategy (inspired by Airflow XCom lookup semantics)
is:

- `producer_job` + `run_id` + `key`
- default `key` is `"return_value"`
- default `run_id` in worker context comes from `DISPATCHIO_RUN_ID`

By default this resolves to a full key like
`"discover_entities/20260115/entities"`.

Users who want a different convention can either:

- pass explicit `full_key` to `get/put/delete`, or
- configure a custom key strategy callable in metadata settings.

### Implementations

Same progression as `StateStore`:

| Class | Use case |
|---|---|
| `MemoryMetadataStore` | Tests, in-process demos |
| `FilesystemMetadataStore` | Local dev (stores JSON files alongside state) |
| `DynamoDBMetadataStore` | Production AWS (future, same phase as DynamoDB StateStore) |

### Harness integration

For a job to write metadata it needs a reference to the store.
The cleanest approach is to inject the store path/config via an env var
(`DISPATCHIO_METADATA_DIR` for filesystem), analogous to how `DISPATCHIO_DROP_DIR`
is injected today. The worker then imports `MetadataStore` from dispatchio and
writes directly:

```python
# Inside a discovery job worker function
from dispatchio.metadata import get_metadata_store

def discover(run_id: str) -> None:
    store = get_metadata_store()   # reads DISPATCHIO_METADATA_DIR from env
    entities = query_database_for_changes(run_id)
    # Default key becomes: discover_entities/<run_id>/entities
    store.push(entities, producer_job="discover_entities", run_id=run_id, key="entities")

def build_jobs(run_id: str) -> list[str]:
    store = get_metadata_store()
    # Equivalent to XCom pull(task_id="discover_entities", key="entities")
    return store.pull(producer_job="discover_entities", run_id=run_id, key="entities") or []
```

### Scope of change

- `dispatchio/metadata/base.py` — `MetadataStore` protocol + `MetadataRecord` model
- `dispatchio/metadata/memory.py` — `MemoryMetadataStore`
- `dispatchio/metadata/filesystem.py` — `FilesystemMetadataStore`
- `dispatchio/metadata/__init__.py` — `get_metadata_store()` factory (reads env) + key strategy wiring
- `dispatchio/executor/python_.py` + `subprocess_.py` — inject `DISPATCHIO_METADATA_DIR`
- `dispatchio/config/settings.py` — optional `[dispatchio.metadata]` config section
- `dispatchio/__init__.py` — re-export `MetadataStore`

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
| 4 — Metadata store | New `dispatchio/metadata/` package, harness injection, 1 example | Phase 3 (factory reads from it) |
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

4. **Metadata store config** — should the metadata store be configured
   under the same `dispatchio.toml` as the state store, or separately?
   Sharing the same section (with sub-keys) seems cleaner.

5. **Hierarchical option B (week_day expressions)** — worth a follow-up
   design pass once the factory pattern is in use and we can see which
   mappings are written repeatedly.

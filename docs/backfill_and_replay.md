# Backfill And Replay - Design Draft

## Overview

This document proposes first-class backfill and replay support in Dispatchio.

The design is intentionally built around a new first-class orchestrator run
concept rather than extending the existing `reference_time` parameter.
That change is the foundation for the rest of the feature set:

- dry-run planning before submit
- bounded replay concurrency
- resume-from-failure checkpoints
- idempotency guardrails
- one orchestrator coordinating both normal and backfill work

This is a breaking change area, but Dispatchio is still in alpha and the
change allows the model to become clearer rather than more complicated.

---

## Goals

- Make backfill and replay a first-class CLI and API capability.
- Avoid requiring concurrent orchestrators for normal scheduling and backfill.
- Preserve Dispatchio's existing single-orchestrator simplicity.
- Keep admission control, pool capacity, and execution ordering in one place.
- Support dry-run planning before any state mutation.
- Make replay resumable after process failure or operator intervention.
- Prevent accidental duplicate backfill submission unless explicitly forced.

## Non-goals

- Introduce a full multi-scheduler or multi-reader architecture.
- Make every cadence replayable by date range.
- Rebuild Dispatchio around a heavyweight Airflow-style database scheduler.
- Change the existing attempt record model as the primary unit of job state.

---

## Current Model And Its Limits

Today, the orchestrator receives an optional `reference_time` on `tick()`.
Each job resolves its own `job_run_key` from that datetime and its cadence.
That works well for single-run date-based scheduling, but it has three limits:

1. `reference_time` is only a wall-clock anchor, not a first-class persisted run.
2. There is no persisted concept of pending, active, blocked, or completed orchestrator runs.
3. Date-range replay has no place to store planning, progress, priority, or resume checkpoints.

The result is that backfill currently fits only as repeated manual ticking,
which is operationally fragile and not expressive enough for first-class use.

---

## Core Design Decision

Introduce a first-class orchestrator run model stored in `StateStore`.

The orchestrator remains the single authority for:

- selecting the active run
- deciding whether scheduled or backfill work takes priority
- applying admission control and pool limits
- advancing runs from pending to active to terminal states

This is a fat orchestrator model. A wrapper-based design was considered, but it
would split run selection from admission control and make replay lifecycle
management harder to reason about.

---

## Terminology

### Orchestrator run

The outer unit of scheduling and replay. An orchestrator run is persisted,
queued, activated, resumed, blocked, and closed.

### Run key

The identity of an orchestrator run. This replaces `reference_time` as the
primary public scheduling concept.

Examples:

- daily date-based orchestrator: `2025-01-15`
- weekly date-based orchestrator: `2025-W03`
- event-based orchestrator: `event:customer-import-9841`
- batch-based orchestrator: `batch:2026-04-21T120000Z`

### Job run key

The existing per-job persisted state key used in `AttemptRecord`.

This remains the key for job attempt history, retries, completion correlation,
and dependency resolution. It is not replaced.

### Orchestrator cadence

The cadence that defines the run unit for the orchestrator itself.

This is distinct from job cadence.

---

## Proposed Mental Model

Dispatchio should model an orchestrator run as the outer scheduling unit, and
job attempts as the inner execution units within that run.

Conceptually:

- orchestrator run answers: what run is currently being worked on
- job run key answers: what state key should this job read and write

For date-based orchestrators, wall clock time can still be mapped to a run key.
For event or batch orchestrators, callers can submit an explicit run key without
pretending that the input is a datetime.

---

## Proposed API Shape

### Orchestrator constructor

Add `orchestrator_cadence` as a first-class field.

Proposed behavior:

- `orchestrator_cadence` defines the run unit for scheduling and replay.
- `default_cadence` remains the fallback job cadence.
- if `default_cadence` is not provided, it defaults to `orchestrator_cadence`.

This preserves convenience while making the outer run model explicit.

### Tick API

Move the public API away from `reference_time` and toward a run key driven
contract.

Proposed conceptual API:

```python
orchestrator.tick()
orchestrator.tick(run_key=...)
orchestrator.plan_backfill(start=..., end=..., dry_run=True)
orchestrator.enqueue_backfill(start=..., end=..., ...)
orchestrator.resume_run(run_id=...)
```

Notes:

- `tick()` with no arguments asks the orchestrator to choose the next active run.
- `tick(run_key=...)` is an explicit override for tests, ad hoc replay, and advanced callers.
- `reference_time` can remain as a compatibility helper for date-based orchestrators,
	but it should no longer be the primary abstraction.

---

## Proposed State Model

Add a new persisted run record to `StateStore`.

### New model: `OrchestratorRunRecord`

Suggested fields:

```python
class OrchestratorRunStatus(str, Enum):
		PENDING = "pending"
		ACTIVE = "active"
		BLOCKED = "blocked"
		COMPLETED = "completed"
		FAILED = "failed"
		CANCELLED = "cancelled"


class OrchestratorRunMode(str, Enum):
		SCHEDULED = "scheduled"
		BACKFILL = "backfill"
		REPLAY = "replay"


class OrchestratorRunRecord(BaseModel):
		orchestrator_run_id: UUID
		orchestrator_name: str
		run_key: str
		status: OrchestratorRunStatus
		mode: OrchestratorRunMode
		priority: int = 0
		submitted_by: str | None = None
		reason: str | None = None
		force: bool = False
		replay_group_id: UUID | None = None
		checkpoint: dict[str, Any] = Field(default_factory=dict)
		opened_at: datetime
		activated_at: datetime | None = None
		closed_at: datetime | None = None
```

### Why a separate run record is needed

`AttemptRecord` cannot represent run-level lifecycle correctly because it is
scoped to individual jobs. Backfill and replay need a record that survives even
before the first job is submitted and after the last job is closed.

### `StateStore` additions

Suggested new protocol methods:

```python
def append_orchestrator_run(self, record: OrchestratorRunRecord) -> None: ...
def get_orchestrator_run(self, orchestrator_run_id: UUID) -> OrchestratorRunRecord | None: ...
def get_orchestrator_run_by_key(self, orchestrator_name: str, run_key: str) -> OrchestratorRunRecord | None: ...
def list_orchestrator_runs(
		self,
		orchestrator_name: str | None = None,
		status: OrchestratorRunStatus | None = None,
		mode: OrchestratorRunMode | None = None,
) -> list[OrchestratorRunRecord]: ...
def update_orchestrator_run(self, record: OrchestratorRunRecord) -> None: ...
```

This keeps run persistence first-class inside the same storage abstraction as
attempts, dead letters, and retry audit records.

---

## Run Key Resolution

### Why not pass a raw logical run id to the orchestrator

The existing `job_run_key` already means the per-job state key. Reusing that
term for the orchestrator would blur two different scopes.

Instead, the orchestrator should work with a run key and derive each job's
logical run id from that outer run.

### Resolution rules

Given:

- an active orchestrator run
- the orchestrator cadence
- the job cadence

Dispatchio resolves the job's persisted `job_run_key`.

Examples:

- daily orchestrator + daily job -> `D20250115`
- daily orchestrator + monthly job -> `M202501`
- daily orchestrator + yesterday dependency -> `D20250114`
- event orchestrator + fixed job cadence -> fixed literal value

For non-date orchestrators, date-derived job cadences may be invalid. That is a
feature, not a bug. It makes unsupported combinations fail early instead of
implicitly guessing.

---

## Replay Capability By Cadence

Replay support should be determined by orchestrator cadence, not assumed for all jobs.

### Date-based orchestrators

Date-based orchestrators support range planning naturally.

Examples:

- hourly: range of hours
- daily: range of days
- weekly: range of weeks
- monthly: range of months

### Event or batch orchestrators

These do not support date-range replay by default.

Instead, they support explicit replay of selected run keys.

Examples:

- replay specific batch ids
- replay explicit event ids
- replay a caller-provided ordered list of run keys

This keeps the semantics honest and avoids pretending every cadence can be
 expressed as a date interval.

---

## Scheduling Model

Dispatchio should support one active orchestrator run at a time, with zero or
more pending runs.

This is the core mechanism that allows backfill without concurrent orchestrators.

### Proposed scheduler rules

At each tick, the orchestrator:

1. loads pending and active orchestrator runs from `StateStore`
2. selects the active run if one already exists
3. if none is active, promotes the highest priority pending run
4. evaluates jobs for that run
5. applies admission control using current global and pool capacity
6. updates checkpoints and run status
7. closes or blocks the run if needed

This preserves one orchestration authority while allowing multiple future runs
to be queued safely.

### Priority between normal and backfill work

The scheduler should support a policy for deciding whether scheduled work or
backfill work wins when both are pending.

This should be explicit, not emergent.

Suggested initial policy options:

- `scheduled_first`
- `backfill_first`
- `explicit_priority`

The default should be conservative and easy to explain. `scheduled_first` is a
reasonable default for production safety.

---

## Run Closure Semantics

Run closure should differ between scheduled runs and backfill or replay runs.

### Scheduled runs

Scheduled runs may be closed when a newer scheduled run becomes eligible.

This preserves the current expected behavior that the orchestrator should not
stay stuck forever on an incomplete prior day.

Practical rule:

- if a new scheduled run becomes current, the prior scheduled run may close even
	if some jobs remain incomplete

### Backfill and replay runs

Backfill and replay runs should not auto-abandon on time rollover.

Practical rule:

- stay ACTIVE while making progress
- move to BLOCKED when progress cannot continue without intervention
- resume from checkpoint when the user retries, unblocks, or resumes
- only move to COMPLETED, FAILED, or CANCELLED explicitly

This matches the operational expectation that backfill should pause cleanly and
resume after manual intervention.

---

## Dry-run Planning

Dry-run planning should be available before any replay or backfill submission.

### Purpose

Allow the operator to answer:

- what run keys will be created
- which jobs are eligible in each run
- which runs already exist
- which runs would be skipped because of idempotency rules
- how concurrency limits and queue priority would affect execution

### Output shape

The dry-run plan should include:

- requested run range or explicit run keys
- resolved run key list
- per-run status: new, already_exists, already_completed, incompatible, skipped
- counts of candidate jobs and already-finished jobs
- whether `--force` would be required

Dry-run must not write orchestrator run records or attempt records.

---

## Concurrency During Replay

The user requested a concurrency cap during replay.

This should be modeled at the orchestrator run scheduler layer, not by launching
multiple orchestrators.

### Recommended behavior

- only one orchestrator process remains responsible for all work
- scheduler may keep one active run at a time initially
- admission control continues to limit job-level parallelism within that run

### Future extension

If needed later, the model can allow a bounded number of simultaneously active
backfill runs while still preserving one orchestrator authority.

Suggested future setting:

```python
max_active_orchestrator_runs: int = 1
```

For the first implementation, `1` is strongly preferred. It fits the current
receiver and orchestration assumptions and already satisfies the main use case.

---

## Resume And Checkpoints

Backfill and replay should be resumable after orchestrator failure or manual pause.

### Checkpoint scope

Checkpoint data belongs to the orchestrator run record.

Examples of checkpoint content:

- last evaluation timestamp
- whether the run ever made forward progress
- counts of finished vs total jobs for that run
- list of unresolved blocking jobs
- operator notes or failure reason

This does not replace job-level state. It augments it with run-level progress.

### Resume behavior

On resume, the orchestrator reloads the run record and re-evaluates jobs using
the same run key. Already-finished attempts stay finished. Pending jobs can be
submitted. Blocked runs can become active again.

This gives a natural resume-from-failure model without inventing duplicate state.

---

## Idempotency Guardrails

There are two layers of idempotency.

### Run-level idempotency

If an orchestrator run already exists for a given `(orchestrator_name, run_key)`:

- dry-run reports the conflict
- enqueue is rejected by default
- `--force` is required to enqueue again

If the existing run is already completed, the rejection message should say so explicitly.

### Job-level idempotency

Dispatchio's existing attempt logic remains in force.

If a job for a given `job_run_key` is already DONE, it is skipped even when
the enclosing orchestrator run is resumed.

Together, these guardrails prevent both accidental duplicate replay requests and
duplicate job submissions inside a run.

---

## CLI Proposal

Backfill and replay should be first-class top-level CLI capabilities.

Suggested command family:

```text
dispatchio run plan
dispatchio run list
dispatchio run show
dispatchio run resume
dispatchio run cancel
dispatchio backfill plan
dispatchio backfill enqueue
dispatchio replay plan
dispatchio replay enqueue
```

### Examples

Date-based backfill dry-run:

```bash
dispatchio backfill plan \
	--orchestrator myjobs:orchestrator \
	--start 2025-01-01 \
	--end 2025-01-31
```

Date-based backfill submit:

```bash
dispatchio backfill enqueue \
	--orchestrator myjobs:orchestrator \
	--start 2025-01-01 \
	--end 2025-01-31 \
	--priority 10
```

Explicit replay by run key:

```bash
dispatchio replay enqueue \
	--orchestrator myjobs:orchestrator \
	--run-key event:customer-import-9841
```

Resume a blocked backfill run:

```bash
dispatchio run resume \
	--context prod \
	--orchestrator-run-id 8f0c... \
	--reason "dependency repaired"
```

### CLI behavior notes

- `plan` commands never mutate state.
- `enqueue` commands create pending orchestrator runs.
- `run list` and `run show` expose run-level lifecycle and checkpoint state.
- existing retry and record commands continue to operate on job-level state.
- future CLI improvements may add run-aware retry helpers on top.

---

## Python API Proposal

The library API should mirror the CLI.

Suggested conceptual API:

```python
plan = orchestrator.plan_backfill(start=..., end=...)
queued = orchestrator.enqueue_backfill(start=..., end=..., submitted_by="cli")
orchestrator.resume_run(orchestrator_run_id=...)
orchestrator.cancel_run(orchestrator_run_id=...)
result = orchestrator.tick()
```

The primary API should return structured result models rather than raw strings
so the CLI and library surfaces share the same semantics.

---

## Interaction With Existing Features

### Dependencies

Dependency resolution continues to be based on job-level run keys.

The difference is that those run keys are now derived from the active
orchestrator run key rather than directly from a raw `reference_time`.

### Completion receivers

No special receiver redesign is required for this feature.

Completion correlation is already attempt-id driven. A single orchestrator can
continue draining the receiver safely while coordinating scheduled and backfill work.

### Retry requests

The current retry model is keyed to job `job_run_key`.

This remains valid, but the operator experience should expose both scopes:

- run-level resume or cancel
- job-level retry within a run

### Manual record overrides

Manual status overrides should remain job-level operations.

Backfill run lifecycle should not be encoded by manually editing attempt rows.

---

## Incremental Delivery Plan

This feature should be delivered in phases.

### Phase 1 - Introduce orchestrator run records

- add run record models and enums
- extend `StateStore`
- add SQLAlchemy schema support
- add run list and get APIs

### Phase 2 - Add orchestrator cadence and run key resolution

- add `orchestrator_cadence`
- define run key model and resolution rules
- make `default_cadence` default to `orchestrator_cadence`
- preserve date-based compatibility helpers

### Phase 3 - Add scheduler queue semantics

- pending and active run selection
- scheduled vs backfill priority policy
- run closure semantics
- checkpoint updates

### Phase 4 - Add CLI and API planning and enqueue commands

- `backfill plan`
- `backfill enqueue`
- `replay plan`
- `replay enqueue`
- `run list`, `run show`, `run resume`, `run cancel`

### Phase 5 - Add operator refinements

- blocked run diagnostics
- richer checkpoint summaries
- optional future support for more than one active orchestrator run

---

## Migration And Compatibility

This is a breaking-change area, but the migration path can stay manageable.

### Compatibility recommendations

- keep `tick(reference_time=...)` temporarily as a compatibility path for
	date-based orchestrators
- internally translate that call into a run key
- mark `reference_time` as a legacy convenience rather than the core model

### What should not change

- `AttemptRecord` remains the source of truth for job execution history
- completion correlation by `dispatchio_attempt_id` remains unchanged
- existing date-based job cadence resolution logic stays conceptually valid

---

## Design Summary

The recommended design is:

- make orchestrator runs first-class persisted records in `StateStore`
- introduce `orchestrator_cadence` as distinct from job cadence
- replace `reference_time` as the primary public concept with a run key driven model
- keep one orchestrator as the single authority over scheduling and admission
- support dry-run planning, resumable runs, and run-level idempotency as built-in features

This preserves Dispatchio's simplicity while giving backfill and replay a model
that is explicit, auditable, and operationally safe.

## Open Questions For Implementation

- Exact serialization format for non-date run keys
- Whether initial `run_key` should remain a plain string in storage even if the
	in-memory API uses typed wrappers
- Whether blocked runs should be a distinct status or a flag attached to active runs
- How much run-level summary data should be persisted versus derived on read

These are implementation questions, not design blockers.

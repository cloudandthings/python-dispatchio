# Concurrency, Pools, and Per-Tick Submit Limits — Design (v1)

## Status

Proposed for alpha. Breaking changes are allowed.

## Summary

This design introduces a simple admission-control model for Dispatchio that protects downstream systems without adding rate-state complexity.

v1 adds:

- Global max active jobs.
- Per-pool max active jobs.
- Global max submissions per tick.
- Per-pool max submissions per tick.
- Explicit defer outcomes in tick results.

v1 intentionally does not add cooldowns, token buckets, or replay/backfill pool routing (future pass).

## Design Goals

- Keep behavior easy to understand for operators.
- Separate capacity limits from pacing limits.
- Preserve deterministic outcomes per tick.
- Avoid persistent limiter state (no token clocks/cooldown timers).

## Non-Goals (v1)

- No distributed pool management across multiple orchestrator instances.
- No cooldown windows.
- No token-bucket or time-based refill logic.
- No tag-based constraints.
- No replay/backfill pool routing (design note only; implementation deferred).

## Scope and Explicit Limitation

Pool limits are enforced only within a single orchestrator instance.

This means:

- If two orchestrator instances run against the same backend, each applies limits independently.
- Effective global/pool limits are not guaranteed across instances.
- This is acceptable for alpha and should be documented in CLI/help/docs.

## Terminology

- Active capacity limit: maximum concurrently active jobs.
- Submit step limit: maximum new submissions admitted in one tick.
- Pool: a single admission lane assigned to each job.
- Active job: a job with an attempt whose status is in `_ACTIVE_STATUSES` (`SUBMITTED`, `QUEUED`, `RUNNING`).

## Pool-First (Not Tags)

Reasons:

- Each job has one pool, which makes admission deterministic and easy to reason about.
- Policy definitions remain simple for users and implementation.
- Tags are powerful for cross-cutting constraints, but increase policy complexity and conflict resolution.

## API and Model Changes (Breaking)

### Job model

Add fields:

- `pool: str = "default"` - Name of the pool, default is "default" pool.
- `priority: int = 0` — higher value means higher admission priority. Jobs with equal priority are ordered by definition order (position in the `jobs` list passed to the orchestrator). Priority only affects admission ordering; it has no effect on scheduling logic (condition evaluation, dependency checks, etc.).

No tag field in v1.

### JobTickResult model

Change field:

- `pool: str = "default"` — always set to the job's pool name at the time the tick result is produced. Applies to all outcomes including condition skips, dependency skips, and deferred actions.

### Orchestrator constructor

Replace current submission-only controls with admission controls.

Current:

- `submit_concurrency`
- `max_submissions_per_tick`

v1 proposal:

- `submit_workers: int = 8` (thread count used after admission)
- `admission_policy: AdmissionPolicy | None = None` — defaults to `AdmissionPolicy()` internally if not supplied

`submit_workers` is execution parallelism only.
Admission caps decide which jobs are allowed to submit.

`submit_timeout` is unchanged — it controls per-submission deadline and is unrelated to admission. It remains a constructor-only parameter and is not part of `AdmissionPolicy`.

### AdmissionPolicy model

`AdmissionPolicy` is a single Python class used both as the constructor argument and as the config-loaded model.

Pools are defined wherever `AdmissionPolicy` is constructed. For code-first users, construct `AdmissionPolicy` directly with a `pools` dict. For config-file users, pools are declared in TOML. Both produce the same object.

Python class shape:

```python
class PoolPolicy(BaseModel):
    max_active_jobs: int | None = None           # None = unlimited
    max_submit_jobs_per_tick: int | None = None  # None = unlimited

class AdmissionPolicy(BaseModel):
    max_active_jobs: int | None = None
    max_submit_jobs_per_tick: int | None = None
    pools: dict[str, PoolPolicy] = Field(default_factory=lambda: {"default": PoolPolicy()})
```

Example in code:

```python
AdmissionPolicy(
    max_active_jobs=200,
    max_submit_jobs_per_tick=50,
    pools={
        "default": PoolPolicy(max_active_jobs=100, max_submit_jobs_per_tick=25),
        "bulk": PoolPolicy(),  # uncapped
    }
)
```

Equivalent TOML:

```toml
[dispatchio.admission]
max_active_jobs = 200
max_submit_jobs_per_tick = 50

[dispatchio.admission.pools.default]
max_active_jobs = 100
max_submit_jobs_per_tick = 25

[dispatchio.admission.pools.bulk]
# no limits — uncapped; existence is enough to make the name valid
```

Notes:

- All limit values are optional. Absence means unlimited (no cap applied).
- This applies to user-defined pools as well — a pool declared without limits is uncapped.
- The default pool has no limits by default. User config overrides it.
- If a job references a pool name not declared in config and not pre-declared in code, fail with an initialization error.

### Settings model (`dispatchio/config/settings.py`)

Replace `SubmissionSettings` with `AdmissionPolicy`.

Config section changes from `[dispatchio.submission]` to `[dispatchio.admission]`.

`submit_timeout` is not represented in `AdmissionPolicy`. It remains a standalone constructor argument only.

### CLI integration

Add flag to the `tick` command:

- `--pool` — filter to only process jobs assigned to this pool in this tick. Jobs in other pools are skipped entirely; they are not deferred. Passing an unknown pool name is a CLI validation error.

## Admission Algorithm

Admission runs after normal planning (condition/dependency/retry checks) and before submit execution.

Input:

- Pending submissions from planning phase.
- Current active attempts from state.
- Admission policy.

Steps:

1. Build a `job_name` to `pool` lookup map from the orchestrator's current job list.

2. Compute active counts.
   - `active_jobs` — count of all attempts whose status is in `_ACTIVE_STATUSES`.
   - `active_jobs_by_pool` — same count broken down by pool, using the lookup map from step 1.
   - Orphaned active attempts (job was removed or renamed since the attempt was created) do count toward `active_jobs` count but do not count toward any pool cap, since their pool is unknown.

3. Initialize per-tick counters (in-memory only, not written to state).
   - `submitted_jobs_this_tick = 0`
   - `submitted_jobs_this_tick_by_pool = defaultdict(int)` — incremented lazily as candidates are admitted.

4. Sort candidates: primary sort by `priority` (descending), secondary sort by definition order (position in the `jobs` list passed to the orchestrator).

5. For each candidate, evaluate gates in order:
   - Global active cap.
   - Pool active cap.
   - Global submit-per-tick cap.
   - Pool submit-per-tick cap.

6. If all gates pass:
   - Admit candidate.
   - Increment active and per-tick counters as reservations for the remainder of this tick.
   - These increments are in-memory only and do not write to the state store.

7. If any gate fails:
   - Defer candidate with the reason code of the **first** failing gate.

8. Collect list of admitted candidates by repeating above step 5+6+7 per candidate.

9. Execute admitted candidates using `submit_workers` thread pool.

## Explicit Tick Outcomes

Today, over-cap jobs can be silently deferred by slicing pending submissions and ignoring jobs that are sliced-off thus excluded from submission.

In v1, every non-admitted candidate should produce an explicit tick result.

Add `JobAction` values (lowercase, matching existing enum convention):

- `deferred_active_limit`
- `deferred_pool_active_limit`
- `deferred_submit_limit`
- `deferred_pool_submit_limit`

For dry-run mode, add:

- `would_defer` — used when dry-run would have deferred a candidate. The `detail` field contains the reason code followed by the counter values, e.g. `"deferred_pool_active_limit pool=replay active=18/20"`.

Detail string format for the live `deferred_*` actions uses inline `key=value` pairs:

- Top-level active cap: `"active=199/200"`
- Pool active cap: `"pool=replay active=18/20"`
- Top-level submit cap: `"submit_jobs_this_tick=50/50"`
- Pool submit cap: `"pool=replay submit_jobs_this_tick=5/5"`

## State Requirements

No new persistent limiter state is required.

v1 relies on:

- Existing attempt statuses (`_ACTIVE_STATUSES`) or `active()` classmethod to derive active counts.
- In-memory counters for submit-per-tick limits during the current tick. These are not written to state.

## Validation Rules

- `max_active_jobs` and `max_submit_jobs_per_tick` (on both `AdmissionPolicy` and `PoolPolicy`) must be positive integers when set.
- The `default` pool is normally valid (pre-declared in code with no limits).
- In future we might allow disabling the default pool with config, but for now, it is always in existence.
- Unknown pool referenced by a job is an error (raised when validating jobs).
- When `allow_runtime_mutation` is enabled, dynamically added jobs are also validated against declared pools at add time as part of job validation.
- Unknown pool passed to `--pool` is a CLI validation error.

## Migration Notes

Breaking changes expected in alpha.

- Old `submission.max_per_tick` is removed.
- Old constructor arg `max_submissions_per_tick` is removed.
- Old constructor arg `submit_concurrency` is replaced by `submit_workers`.
- `submit_timeout` constructor arg is unchanged.
- Config section `[dispatchio.submission]` is replaced by `[dispatchio.admission]`. The `timeout` field from `SubmissionSettings` has no equivalent in `AdmissionPolicy`; users relying on it must pass `submit_timeout` directly to the orchestrator constructor.
- Config migration should be documented with before/after snippets.

Optional temporary compatibility shim can be skipped to keep implementation clean, because we are in alpha so breaking changes are allowed.

## Testing Plan

Unit tests:

- Admits within all limits.
- Defers when global active cap reached.
- Defers when pool active cap reached.
- Defers when global per-tick submit cap reached.
- Defers when pool per-tick submit cap reached.
- Candidate ordering: higher priority admitted first; equal priority respects definition order.
- `JobTickResult.pool` is populated for all outcomes including skips and deferred.
- Pool declared with no limits is uncapped.
- Unknown pool validation at init.
- Unknown pool validation when adding jobs dynamically.
- Orphaned active attempts count toward global cap only.

Integration tests:

- Mixed pools in same tick produce expected admitted/deferred actions.

CLI output tests:

- Deferred actions are shown explicitly with detail strings.
- `would_defer` shown in dry-run with reason code and counter detail.

## Implementation Plan

1. Add `PoolPolicy` and `AdmissionPolicy` models and loader wiring (replaces `SubmissionSettings`).
2. Add `pool` and `priority` to `Job`; validate pool membership at init and add time.
3. Update `JobTickResult.pool` to `str = "default"`.
4. Introduce admission engine module.
5. Replace simple pending-slice logic in orchestrator with admission decisions.
6. Add new `JobAction` deferred enums, `would_defer`, and output formatting.
7. Add `--pool` flag to `tick` CLI command.
8. Update docs and examples.
9. Add test coverage.

## Future Extensions (Post-v1)

- Replay/backfill pool routing.
- Tags for cross-cutting limits.
- Cooldown windows.
- Token-bucket style rate control.
- Multi-instance coordinated pool enforcement using transactional backend semantics.

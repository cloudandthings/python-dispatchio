# Suggested features - developer

# Known issues

LambdaExecutor has no poke(), so if a Lambda worker crashes without posting a completion event it will sit in SUBMITTED forever unless you add a timeout/lost-job policy for it.
We could make the default timeout equal to greater than the lambda timeout but lambda is resumable now.

_PendingSubmission and other code does not use submitted_by

## Namespace / identity migration tooling

Now that all state records are keyed by `namespace` (derived from orchestrator name),
renaming an orchestrator or a job orphans existing records — the new name can't see
prior runs.

Two operations are needed (details TBD, implementation deferred):

**Orchestrator rename** (`namespace` migration)

- An operator renames their orchestrator from `"pipeline-v1"` to `"pipeline-v2"`.
- All `AttemptRecord`, `DeadLetterRecord`, `Event`, and `OrchestratorRunRecord` rows
  with `namespace = "pipeline-v1"` must be rewritten to `namespace = "pipeline-v2"`.
- CLI sketch: `dispatchio migrate rename-namespace OLD NEW`
- Implementation: targeted `UPDATE ... SET namespace = ? WHERE namespace = ?` across
  all affected tables. Wrap in a transaction.

**Job rename** (within a namespace)

- An operator renames a job from `"ingest_raw"` to `"ingest"` within the same namespace.
- `AttemptRecord` rows for the old job name must be rewritten to the new name.
- The `correlation_id` on each row is globally unique and does not change.
- Downstream `JobDependency` references to the old name also need updating — those live
  in code, not state, so this is a two-step: rename in code then migrate state.
- CLI sketch: `dispatchio migrate rename-job NAMESPACE OLD_JOB_NAME NEW_JOB_NAME`
- Open question: should historical records keep the old name as an audit alias?

Both operations are effectively idempotent `UPDATE` statements. The hard part is
making the CLI safe to run against a live database (quiesce in-flight attempts first,
or add optimistic locking).

---

other renames

dependency state caching

tick log in database ?

Auto delete metadata from previous attempts ?

default when duplicate completion events, just update to latest, keep audit. Or global setting to control it.

superbatch?
- tracing

perf tests?

8. Calendar-aware scheduling
Why: Business calendars are a frequent need in finance and ops.
Include:
- Holiday calendars, market/business-day policies, timezone-aware windows.
- “Nth business day” and “last business day” conditions.
Impact: Medium. Effort: Medium.

10. Lightweight web console (read-first)
Why: Improves usability for operators without replacing CLI.
Include:
- Tick timeline, DAG state view, attempt history, manual retry/cancel actions.
- Start read-only, then add guarded mutation actions.
Impact: Medium. Effort: High.



# Suggested features - copilot

Suggested new features (prioritized)
1. Backfill and date-range replay as first-class CLI/API
Why: This is the highest operational value for real ETL usage.
Include:
- Dry-run planning before submit.
- Concurrency cap during replay.
- Resume-from-failure checkpoints.
- Idempotency guardrails.
Impact: High. Effort: Medium.

2. Cascading skip with explicit reason codes
Why: Reduces noise and makes terminal dependency failures easier to interpret.
Include:
- Policy toggle per job or global.
- Structured reason propagation to downstream records.
- Optional alert for “skipped due to upstream permanent failure.”
Impact: High. Effort: Medium.

3. Concurrency, pools, and rate-limiting controls
============== DONE ===================
Why: Critical for protecting downstream systems.
Include:
- Global max active jobs.
- Per-tag/per-pool limits.
- Cooldown windows and token-bucket style throttling.
Impact: High. Effort: Medium.

4. Native backfill-safe “latest successful dependency” mode
Why: Common requirement when upstream cadence differs or data is late.
Include:
- Dependency can resolve to most recent DONE run within lookback window.
- Deterministic tie-break rules.
Impact: High. Effort: Medium.

5. Observability package: metrics + traces + structured events
Why: Makes production operations easier than log-only workflows.
Include:
- Prometheus/OpenTelemetry counters and histograms.
- Tick duration, queue lag, retries, lost detection, dependency wait time.
- Attempt correlation IDs across orchestrator and workers.
Impact: High. Effort: Medium.

6. Dead-letter workflow and reprocessing tools
Why: You already have audit concepts; this closes the incident loop.
Include:
- Dead-letter store for malformed/mismatched completion events.
- CLI inspect/replay/quarantine actions.
Impact: Medium-high. Effort: Medium.

7. DataStore backends for production scale
Why: Filesystem and memory are great locally, but many teams need distributed durability.
Include:
- SQLite and DynamoDB backends.
- Optional retention TTL and garbage collection.
- Bulk read/write for fan-out/fan-in efficiency.
Impact: Medium-high. Effort: Medium.

8. Calendar-aware scheduling
Why: Business calendars are a frequent need in finance and ops.
Include:
- Holiday calendars, market/business-day policies, timezone-aware windows.
- “Nth business day” and “last business day” conditions.
Impact: Medium. Effort: Medium.

9. Policy and safety layer for graph admission
Why: Important for large teams using dynamic registration or JSON artifacts.
Include:
- Lint rules (naming, owner tags, retry bounds, forbidden executor options).
- Admission checks before a graph is accepted.
Impact: Medium. Effort: Low-medium.

10. Lightweight web console (read-first)
Why: Improves usability for operators without replacing CLI.
Include:
- Tick timeline, DAG state view, attempt history, manual retry/cancel actions.
- Start read-only, then add guarded mutation actions.
Impact: Medium. Effort: High.

If you want, I can turn this into a concrete 30/60/90-day roadmap with acceptance criteria and test plan per feature.

# Suggested features - developer

dependency state caching


tick log in database ?

Auto delete metadata from previous attempts ?

default when duplicate completion events, just update to latest, keep audit. Or global setting to control it.

superbatch?
- tracing

perf tests?

typer?

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

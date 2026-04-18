# Retries, Attempts, and Audit Workflows

This guide is for operators who need to inspect attempt history, trigger manual retries, and audit completion mismatches.

## Core model

For each `(job_name, logical_run_id)`, Dispatchio stores immutable attempt records:

- `attempt`: 0, 1, 2, ...
- `dispatchio_attempt_id`: UUID for exact correlation
- `trigger_type`: `scheduled`, `auto_retry`, or `manual_retry`
- `trigger_reason`: optional human/operator context
- `trace`: executor/upstream metadata

Use attempt history (not just latest status) when debugging incidents.

## Inspect attempts for one logical run

```python
attempts = orchestrator.state.list_attempts(
    job_name="ingest",
    logical_run_id="20260418",
)
for a in attempts:
    print(
        a.job_name,
        a.logical_run_id,
        a.attempt,
        a.status.value,
        a.trigger_type.value,
        a.trigger_reason,
        a.dispatchio_attempt_id,
    )
```

## Create a manual retry

Manual retry creates the next attempt for a terminal job attempt.

```python
new_attempt = orchestrator.manual_retry(
    job_name="ingest",
    logical_run_id="20260418",
    operator_name="oncall-alice",
    operator_reason="upstream partition repaired",
)
print(new_attempt.attempt, new_attempt.trigger_type.value)
```

Expected behavior:

- New attempt has incremented attempt number.
- New attempt has `trigger_type=manual_retry`.
- Operator identity and reason are persisted for audit.

## Cancel an active attempt

```python
updated = orchestrator.manual_cancel(
    dispatchio_attempt_id=attempt_id,
    operator_name="oncall-alice",
    operator_reason="stuck downstream API; safe to stop",
)
print(updated.status.value, updated.operator_name)
```

Expected behavior:

- Active attempts move to `cancelled`.
- Operator reason is persisted.

## Completion mismatch and dead-letter audit

When completion events include a `dispatchio_attempt_id` that does not match stored identity, Dispatchio:

1. Keeps attempt state unchanged.
2. Appends a dead-letter audit record.
3. Logs/alerts for operator action.

Inspect dead letters:

```python
dead_letters = orchestrator.state.list_dead_letters()
for d in dead_letters:
    print(d.reason_code.value, d.job_name, d.logical_run_id, d.dispatchio_attempt_id)
```

## What to look for during incidents

- Multiple attempts ending in `error` with identical reason text: likely deterministic code/input issue.
- `auto_retry` attempts that eventually succeed: likely transient infra issue.
- Repeated dead-letter identity mismatches: stale/misrouted completion producers.
- Frequent operator-triggered retries/cancels on one job: workflow or dependency health issue.

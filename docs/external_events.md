# Event Dependencies

This guide shows how to trigger Dispatchio jobs from events while keeping
Dispatchio tick-based scheduling unchanged.

The pattern is simple: event producers publish completion-style events using
namespaced dependency names (for example, `event.user_registered`).
Downstream jobs depend on those names exactly like normal dependencies.

## Why this pattern

- Reuses existing receiver backends (filesystem or SQS)
- No new executor type required
- Supports one-event and multi-event fan-in with normal dependency semantics
- Keeps event-to-start delay bounded by your existing tick interval

## Event shape

Use the existing `CompletionEvent` payload shape:

```json
{
  "job_name": "event.user_registered",
  "run_id": "D20250115",
  "status": "done",
  "metadata": {"source": "identity-service"}
}
```

For two-event fan-in, emit two separate events with different `job_name` values
and the same `run_id`.

## Producer helper API

Use `report_external_done()` to publish external dependency events without
manually constructing payloads.

```python
from dispatchio.completion import report_external_done

report_external_done(
  event_name="event.user_registered",
  run_id="D20250115",
  metadata={"source": "identity-service"},
)
```

The helper uses `DISPATCHIO_RECEIVER__*` environment variables by default and
works with both filesystem and SQS backends.

For explicit config, pass `receiver_settings=ReceiverSettings(...)`.

## Optional external dependency registry

Event dependencies are not executable jobs, so use the built-in
`EventDependencySpec` plus `validate_event_dependencies()` helper to
prevent typos and improve discoverability.

Recommended fields:

- `name`: dependency key (for example, `event.user_registered`)
- `cadence`: run_id contract (for example, `daily (DYYYYMMDD)`)
- `description`: source system and business meaning

Use `event_dependency()` to declare event dependencies and
`validate_event_dependencies()` to assert every `event.*` dependency is
registered.

## Orchestrator setting

Set `strict_dependencies=False` when using event dependencies that are not
locally registered executable jobs.

```python
orchestrator = orchestrator_from_config(
    JOBS,
    strict_dependencies=False,
)
```

## Example

See the runnable example:

- `examples/event_dependencies/jobs.py`
- `examples/event_dependencies/run.py`

The example demonstrates:

1. Single event dependency (`send_welcome_email`)
2. Two-event fan-in (`activate_paid_features`)
3. Optional event dependency registry validation

## Optional SQS producer example

If you are using `dispatchio[aws]`, see:

- `examples/aws_lambda/external_event_producer.py`

This script sends both `event.user_registered` and `event.kyc_passed`
events for a given run ID using the same helper API.

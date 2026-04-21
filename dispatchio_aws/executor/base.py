from __future__ import annotations

from typing import Any

from dispatchio.models import AttemptRecord


def build_execution_context(
    attempt: AttemptRecord, reference_time_iso: str
) -> dict[str, str]:
    """Build the context dict injected into executor payloads.

    Includes all Phase 2 attempt identity fields so jobs can report back
    with the correct correlation_id, run_key, and attempt.
    """
    return {
        "job_name": attempt.job_name,
        "run_key": attempt.run_key,
        "attempt": str(attempt.attempt),
        "correlation_id": str(attempt.correlation_id),
        "reference_time": reference_time_iso,
    }


def render_payload(
    payload_template: dict[str, Any] | None,
    context: dict[str, str],
) -> dict[str, Any]:
    if payload_template is None:
        return dict(context)

    return _render_value(payload_template, context)


def _render_value(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        return value.format(**context)
    if isinstance(value, list):
        return [_render_value(item, context) for item in value]
    if isinstance(value, dict):
        return {k: _render_value(v, context) for k, v in value.items()}
    return value

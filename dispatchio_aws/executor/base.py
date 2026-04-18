from __future__ import annotations

from typing import Any


def build_execution_context(
    job_name: str, run_id: str, reference_time_iso: str
) -> dict[str, str]:
    return {
        "job_name": job_name,
        "run_id": run_id,
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

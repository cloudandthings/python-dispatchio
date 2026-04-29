from __future__ import annotations

from typing import Any, TypedDict
from collections.abc import Callable

from beartype import beartype

from dispatchio.config.loader import load_config
from dispatchio.models import Attempt, Status
from dispatchio.reporter import build_reporter
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore

# Module-level singleton — reused across warm Lambda invocations so the
# SQLAlchemy connection pool is not recreated on every event.
_store: SQLAlchemyStateStore | None = None


class AttemptStatusUpdate(TypedDict):
    """What an event handler must return for the completion handler to act on."""

    trace_key: str  # key within attempt.trace["executor"] to look up
    trace_value: str  # the identifier stored at submission time
    status: Status


EventSourceHandler = Callable[[dict[str, Any]], AttemptStatusUpdate | None]
"""Callable signature for a single-source EventBridge completion handler.

Receives the raw EventBridge event dict and returns a :class:`AttemptStatusUpdate`
describing how to look up the attempt and what status to apply.
"""

# ── Built-in source handlers ──────────────────────────────────────────────────


def _handle_stepfunctions_event(event: dict[str, Any]) -> AttemptStatusUpdate | None:
    detail = event.get("detail", {})
    execution_arn = detail.get("executionArn") or detail.get("execution_arn")
    if not execution_arn:
        raise ValueError("Step Functions event detail.executionArn is required")
    return AttemptStatusUpdate(
        trace_key="execution_arn",
        trace_value=execution_arn,
        status=_stepfunctions_status(detail.get("status", "")),
    )


def _handle_athena_event(event: dict[str, Any]) -> AttemptStatusUpdate | None:
    detail = event.get("detail", {})
    query_execution_id = detail.get("queryExecutionId")
    if not query_execution_id:
        raise ValueError("Athena event detail.queryExecutionId is required")
    return AttemptStatusUpdate(
        trace_key="query_execution_id",
        trace_value=query_execution_id,
        status=_athena_status(detail.get("currentState", "")),
    )


_BUILTIN_HANDLERS: dict[str, EventSourceHandler] = {
    "aws.states": _handle_stepfunctions_event,
    "aws.athena": _handle_athena_event,
}

# ── Factory ───────────────────────────────────────────────────────────────────


def build_eventbridge_handler(
    extra_handlers: dict[str, EventSourceHandler] | None = None,
) -> Callable[[dict[str, Any], Any], dict[str, Any]]:
    """Return a Lambda handler that routes EventBridge completion events.

    Built-in sources:

    - ``aws.athena``  — Athena query state changes
    - ``aws.states``  — Step Functions execution state changes

    Pass ``extra_handlers`` to support additional EventBridge sources or to
    override the status mapping for a built-in source.  Keys are EventBridge
    ``source`` field values; values are :data:`EventSourceHandler`
    callables.

    Example::

        def my_glue_handler(event: dict[str, Any]) -> AttemptStatusUpdate:
            detail = event["detail"]
            return AttemptStatusUpdate(
                trace_key="glue_job_run_id",
                trace_value=detail["jobRunId"],
                status=Status.DONE if detail["state"] == "SUCCEEDED" else Status.ERROR,
            )

        handler = build_eventbridge_handler(
            extra_handlers={"aws.glue": my_glue_handler}
        )
    """
    handlers: dict[str, EventSourceHandler] = {
        **_BUILTIN_HANDLERS,
        **(extra_handlers or {}),
    }

    @beartype
    def _handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
        source = event.get("source")
        event_handler = handlers.get(source)  # type: ignore[arg-type]
        if event_handler is None:
            raise ValueError(f"Unsupported event source: {source!r}")

        result = event_handler(event)
        if result is None:
            return {"status": "ignored", "source": source}

        record = _lookup_attempt(result["trace_key"], result["trace_value"])
        _report_completion(record=record, status=result["status"])

        return {
            "status": "ok",
            "source": source,
            "job_name": record.job_name,
            "run_key": record.run_key,
            "attempt": record.attempt,
            "correlation_id": str(record.correlation_id),
        }

    return _handler


# Default module-level handler — backwards-compatible entry point for projects
# that don't need custom sources.
handler = build_eventbridge_handler()

# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_store() -> SQLAlchemyStateStore:
    global _store
    if _store is None:
        cfg = load_config().state
        _store = SQLAlchemyStateStore(connection_string=cfg.connection_string)
    return _store


def _lookup_attempt(trace_key: str, trace_value: str) -> Attempt:
    record = _get_store().get_attempt_by_executor_trace(trace_key, trace_value)
    if record is None:
        raise LookupError(f"No attempt record found for {trace_key}={trace_value!r}")
    return record


def _report_completion(record: Attempt, status: Status) -> None:
    reporter = build_reporter(load_config().receiver)
    if reporter is None:
        raise RuntimeError(
            "No reporter configured in dispatchio settings. "
            "Set receiver.backend to filesystem or sqs."
        )
    reporter.report(correlation_id=record.correlation_id, status=status)


def _stepfunctions_status(status: str) -> Status:
    if status in {"RUNNING", "PENDING_REDRIVE"}:
        return Status.RUNNING
    if status == "SUCCEEDED":
        return Status.DONE
    return Status.ERROR


def _athena_status(status: str) -> Status:
    if status in {"QUEUED", "RUNNING"}:
        return Status.RUNNING
    if status == "SUCCEEDED":
        return Status.DONE
    return Status.ERROR

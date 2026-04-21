from __future__ import annotations

import os
from typing import Any

from beartype import beartype

from dispatchio.config.settings import DispatchioSettings
from dispatchio.models import AttemptRecord, Status
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore
from dispatchio_aws.reporter.sqs import SQSReporter

# Module-level singleton — reused across warm Lambda invocations so the
# SQLAlchemy connection pool is not recreated on every event.
_store: SQLAlchemyStateStore | None = None


def _get_store() -> SQLAlchemyStateStore:
    global _store
    if _store is None:
        cfg = DispatchioSettings().state
        _store = SQLAlchemyStateStore(connection_string=cfg.connection_string)
    return _store


@beartype
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    source = event.get("source")
    if source == "aws.states":
        return _handle_stepfunctions_event(event)
    if source == "aws.athena":
        return _handle_athena_event(event)
    raise ValueError(f"Unsupported event source: {source!r}")


def _handle_stepfunctions_event(event: dict[str, Any]) -> dict[str, Any]:
    detail = event.get("detail", {})
    status = _stepfunctions_status_to_dispatchio(detail.get("status", ""))
    execution_arn = detail.get("executionArn") or detail.get("execution_arn")
    if not execution_arn:
        raise ValueError("Step Functions event detail.executionArn is required")

    record = _lookup_attempt_by_execution_arn(execution_arn)
    _report_completion_from_record(record=record, status=status)

    return {
        "status": "ok",
        "source": "aws.states",
        "job_name": record.job_name,
        "run_key": record.run_key,
        "attempt": record.attempt,
        "correlation_id": str(record.correlation_id),
    }


def _handle_athena_event(event: dict[str, Any]) -> dict[str, Any]:
    detail = event.get("detail", {})
    status = _athena_status_to_dispatchio(detail.get("currentState", ""))
    query_execution_id = detail.get("queryExecutionId")
    if not query_execution_id:
        raise ValueError("Athena event detail.queryExecutionId is required")

    record = _lookup_attempt_by_query_execution_id(query_execution_id)
    _report_completion_from_record(record=record, status=status)

    return {
        "status": "ok",
        "source": "aws.athena",
        "job_name": record.job_name,
        "run_key": record.run_key,
        "attempt": record.attempt,
        "correlation_id": str(record.correlation_id),
    }


def _lookup_attempt_by_execution_arn(execution_arn: str) -> AttemptRecord:
    """Look up an attempt by execution_arn stored in trace.executor at submission time."""
    record = _get_store().get_attempt_by_executor_trace("execution_arn", execution_arn)
    if record is None:
        raise LookupError(
            f"No attempt record found for execution_arn={execution_arn!r}"
        )
    return record


def _lookup_attempt_by_query_execution_id(query_execution_id: str) -> AttemptRecord:
    """Look up an attempt by query_execution_id stored in trace.executor at submission time."""
    record = _get_store().get_attempt_by_executor_trace(
        "query_execution_id", query_execution_id
    )
    if record is None:
        raise LookupError(
            f"No attempt record found for query_execution_id={query_execution_id!r}"
        )
    return record


def _report_completion_from_record(record: AttemptRecord, status: Status) -> None:
    reporter = SQSReporter(
        queue_url=_required_env(
            "DISPATCHIO_RECEIVER__QUEUE_URL", "DISPATCHIO_SQS_QUEUE_URL"
        ),
        region=_optional_env("DISPATCHIO_RECEIVER__REGION", "DISPATCHIO_SQS_REGION"),
    )
    reporter.report(
        correlation_id=record.correlation_id,
        status=status,
    )


def _required_env(primary: str, fallback: str) -> str:
    value = os.environ.get(primary) or os.environ.get(fallback)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {primary} (or {fallback})"
        )
    return value


def _optional_env(primary: str, fallback: str) -> str | None:
    return os.environ.get(primary) or os.environ.get(fallback)


def _stepfunctions_status_to_dispatchio(status: str) -> Status:
    if status in {"RUNNING", "PENDING_REDRIVE"}:
        return Status.RUNNING
    if status == "SUCCEEDED":
        return Status.DONE
    return Status.ERROR


def _athena_status_to_dispatchio(status: str) -> Status:
    if status in {"QUEUED", "RUNNING"}:
        return Status.RUNNING
    if status == "SUCCEEDED":
        return Status.DONE
    return Status.ERROR

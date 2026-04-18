from __future__ import annotations

from typing import Any

from beartype import beartype

from dispatchio.config.settings import DispatchioSettings
from dispatchio.models import Status
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore
from dispatchio_aws.reporter.sqs import SQSReporter


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
    execution_name = str(detail.get("name", ""))

    if "--" not in execution_name:
        raise ValueError(
            "Step Functions event detail.name must be '<job_name>--<run_id>'"
        )
    job_name, run_id = execution_name.split("--", 1)
    _report_completion(job_name=job_name, run_id=run_id, status=status)

    return {
        "status": "ok",
        "source": "aws.states",
        "job_name": job_name,
        "run_id": run_id,
    }


def _handle_athena_event(event: dict[str, Any]) -> dict[str, Any]:
    detail = event.get("detail", {})
    status = _athena_status_to_dispatchio(detail.get("currentState", ""))
    query_execution_id = detail.get("queryExecutionId")
    if not query_execution_id:
        raise ValueError("Athena event detail.queryExecutionId is required")

    job_name, run_id = _resolve_job_context_from_query_execution_id(query_execution_id)
    _report_completion(job_name=job_name, run_id=run_id, status=status)

    return {
        "status": "ok",
        "source": "aws.athena",
        "job_name": job_name,
        "run_id": run_id,
    }


def _resolve_job_context_from_query_execution_id(
    query_execution_id: str,
) -> tuple[str, str]:
    cfg = DispatchioSettings().state
    store = SQLAlchemyStateStore(connection_string=cfg.connection_string)
    for record in store.list_records(status=Status.RUNNING):
        if record.metadata.get("query_execution_id") == query_execution_id:
            return record.job_name, record.run_id
        if (
            record.executor_reference
            and record.executor_reference.get("query_execution_id")
            == query_execution_id
        ):
            return record.job_name, record.run_id

    raise LookupError(
        f"No running record found for Athena query_execution_id={query_execution_id!r}"
    )


def _report_completion(job_name: str, run_id: str, status: Status) -> None:
    reporter = SQSReporter(
        queue_url=_required_env(
            "DISPATCHIO_RECEIVER__QUEUE_URL", "DISPATCHIO_SQS_QUEUE_URL"
        ),
        region=_optional_env("DISPATCHIO_RECEIVER__REGION", "DISPATCHIO_SQS_REGION"),
    )
    reporter.report(job_name=job_name, run_id=run_id, status=status)


def _required_env(primary: str, fallback: str) -> str:
    import os

    value = os.environ.get(primary) or os.environ.get(fallback)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {primary} (or {fallback})"
        )
    return value


def _optional_env(primary: str, fallback: str) -> str | None:
    import os

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

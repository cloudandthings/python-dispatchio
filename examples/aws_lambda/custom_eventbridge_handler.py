"""Custom EventBridge handler: adding AWS Glue support.

By default the EventBridge handler Lambda understands two EventBridge sources:

- ``aws.athena``  — Athena query state changes
- ``aws.states``  — Step Functions execution state changes

Use :func:`build_eventbridge_handler` to extend it with additional sources,
or to override the status mapping for an existing one.

This example adds support for AWS Glue job state changes.  The Glue executor
(not shown) must store ``glue_job_run_id`` in ``attempt.trace["executor"]``
at submission time so the lookup can find the right attempt.

AWS-side wiring
---------------
Add an EventBridge rule that routes Glue events to this Lambda::

    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail      = { state = ["SUCCEEDED", "FAILED", "ERROR", "STOPPED"] }

Configure the Lambda entry point as::

    examples.aws_lambda.custom_eventbridge_handler.handler

Environment variables are the same as the orchestrator Lambda:

    DISPATCHIO_CONFIG=ssm:///myapp/dispatchio
"""

from __future__ import annotations

from typing import Any

from dispatchio.models import Status
from dispatchio_aws.worker.eventbridge_handler import (
    EventSourceHandler,
    AttemptStatusUpdate,
    build_eventbridge_handler,
)


def _glue_handler(event: dict[str, Any]) -> AttemptStatusUpdate | None:
    detail = event.get("detail", {})
    job_run_id = detail.get("jobRunId")
    if not job_run_id:
        raise ValueError("Glue event detail.jobRunId is required")
    state = detail.get("state", "")
    if state == "SUCCEEDED":
        status = Status.DONE
    elif state in {"RUNNING", "STARTING", "STOPPING"}:
        status = Status.RUNNING
    else:
        status = Status.ERROR
    return AttemptStatusUpdate(
        trace_key="glue_job_run_id",
        trace_value=job_run_id,
        status=status,
    )


# Wire the built-in handlers (Athena, Step Functions) together with the custom
# Glue handler and expose the result as the Lambda entry point.
_extra_handlers: dict[str, EventSourceHandler] = {
    "aws.glue": _glue_handler,
}

handler = build_eventbridge_handler(extra_handlers=_extra_handlers)

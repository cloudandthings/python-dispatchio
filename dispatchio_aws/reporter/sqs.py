from __future__ import annotations

import json
import logging
from typing import Any

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.models import Status
from dispatchio.receiver.base import CompletionEvent

logger = logging.getLogger(__name__)


@beartype
class SQSReporter:
    """Post completion events to SQS from job workers."""

    def __init__(
        self,
        queue_url: str,
        region: str | None = None,
        *,
        client: BaseClient | None = None,
    ) -> None:
        self._queue_url = queue_url
        self._client = client or boto3.client("sqs", region_name=region)

    def report(
        self,
        job_name: str,
        run_id: str,
        status: Status,
        *,
        error_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            event = CompletionEvent(
                job_name=job_name,
                run_id=run_id,
                status=status,
                error_reason=error_reason,
                metadata=metadata or {},
            )
            self._client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(event.model_dump(mode="json")),
            )
            logger.info("Reported %s/%s -> %s via SQS", job_name, run_id, status.value)
        except Exception:
            logger.exception(
                "SQSReporter failed to post event for %s/%s", job_name, run_id
            )

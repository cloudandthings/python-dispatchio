from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.models import Status
from dispatchio.receiver.base import StatusEvent
from dispatchio.worker.reporter.base import BaseReporter

logger = logging.getLogger(__name__)


@beartype
class SQSReporter(BaseReporter):
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
        correlation_id: str | UUID,
        status: Status,
        *,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            uuid = (
                UUID(correlation_id)
                if isinstance(correlation_id, str)
                else correlation_id
            )
            event = StatusEvent(
                correlation_id=uuid,
                status=status,
                reason=reason,
                metadata=metadata or {},
            )
            self._client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(event.model_dump(mode="json")),
            )
            logger.info("Reported %s -> %s via SQS", correlation_id, status.value)
        except Exception:
            logger.exception("SQSReporter failed to post event for %s", correlation_id)

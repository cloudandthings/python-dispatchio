from __future__ import annotations

import json
import logging
from typing import Any

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.receiver.base import StatusEvent

logger = logging.getLogger(__name__)


@beartype
class SQSReceiver:
    """Poll completion events from SQS and convert them to StatusEvent objects."""

    def __init__(
        self,
        queue_url: str,
        region: str | None = None,
        *,
        wait_time_seconds: int = 0,
        visibility_timeout: int | None = None,
        max_batch_size: int = 10,
        client: BaseClient | None = None,
    ) -> None:
        self._queue_url = queue_url
        self._wait_time_seconds = wait_time_seconds
        self._visibility_timeout = visibility_timeout
        self._max_batch_size = max(1, min(max_batch_size, 10))
        self._client = client or boto3.client("sqs", region_name=region)

    def drain(self) -> list[StatusEvent]:
        events: list[StatusEvent] = []

        while True:
            messages = self._receive_messages()
            if not messages:
                break

            delete_entries: list[dict[str, str]] = []
            for idx, message in enumerate(messages):
                receipt_handle = message.get("ReceiptHandle")
                if receipt_handle:
                    delete_entries.append(
                        {"Id": str(idx), "ReceiptHandle": receipt_handle}
                    )

                raw_body = message.get("Body", "")
                try:
                    payload: Any = json.loads(raw_body)
                    event = StatusEvent.model_validate(payload)
                    events.append(event)
                except Exception as exc:
                    logger.warning("Skipping malformed SQS completion message: %s", exc)

            if delete_entries:
                self._client.delete_message_batch(
                    QueueUrl=self._queue_url,
                    Entries=delete_entries,
                )

        return events

    def _receive_messages(self) -> list[dict[str, Any]]:
        request: dict[str, Any] = {
            "QueueUrl": self._queue_url,
            "MaxNumberOfMessages": self._max_batch_size,
            "WaitTimeSeconds": self._wait_time_seconds,
        }
        if self._visibility_timeout is not None:
            request["VisibilityTimeout"] = self._visibility_timeout

        response = self._client.receive_message(**request)
        return response.get("Messages", [])

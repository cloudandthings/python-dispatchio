from __future__ import annotations

import json
from uuid import uuid4

import boto3
from moto import mock_aws

from dispatchio.models import Status
from dispatchio_aws.receiver.sqs import SQSReceiver


@mock_aws
def test_drain_returns_status_events_and_clears_queue() -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-status-events")["QueueUrl"]

    correlation_id = uuid4()
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(
            {"correlation_id": str(correlation_id), "status": Status.DONE.value}
        ),
    )

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].correlation_id == correlation_id
    assert events[0].status == Status.DONE

    assert receiver.drain() == []


@mock_aws
def test_drain_skips_malformed_messages() -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    sqs.send_message(QueueUrl=queue_url, MessageBody="not-json")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"job_name": "a", "run_key": "20260414"}),
    )

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert events == []
    assert receiver.drain() == []


@mock_aws
def test_drain_handles_multiple_batches() -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    for _ in range(23):
        correlation_id = uuid4()
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "correlation_id": str(correlation_id),
                    "status": Status.DONE.value,
                }
            ),
        )

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 23
    assert all(event.status == Status.DONE for event in events)

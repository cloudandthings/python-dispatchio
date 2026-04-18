from __future__ import annotations

import json

import boto3
from moto import mock_aws

from dispatchio.models import Status
from dispatchio_aws.receiver.sqs import SQSReceiver


@mock_aws
def test_drain_returns_completion_events_and_clears_queue() -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(
            {"job_name": "ingest", "run_id": "20260414", "status": "done"}
        ),
    )

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].job_name == "ingest"
    assert events[0].run_id == "20260414"
    assert events[0].status == Status.DONE

    assert receiver.drain() == []


@mock_aws
def test_drain_skips_malformed_messages() -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    sqs.send_message(QueueUrl=queue_url, MessageBody="not-json")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"job_name": "a", "run_id": "20260414"}),
    )

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert events == []
    assert receiver.drain() == []


@mock_aws
def test_drain_handles_multiple_batches() -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    for i in range(23):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "job_name": "job",
                    "run_id": f"20260414-{i}",
                    "status": "done",
                }
            ),
        )

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 23
    assert all(event.status == Status.DONE for event in events)

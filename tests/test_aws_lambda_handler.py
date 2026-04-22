from __future__ import annotations

from uuid import uuid4

import boto3
from moto import mock_aws

from dispatchio.models import Status
from dispatchio_aws.receiver.sqs import SQSReceiver
from dispatchio_aws.worker.lambda_handler import dispatchio_handler


@mock_aws
def test_dispatchio_handler_posts_done_event(monkeypatch) -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "sqs")
    monkeypatch.setenv("DISPATCHIO_RECEIVER__QUEUE_URL", queue_url)
    monkeypatch.setenv("DISPATCHIO_RECEIVER__REGION", "eu-west-1")

    correlation_id = uuid4()
    monkeypatch.setenv("DISPATCHIO_CORRELATION_ID", str(correlation_id))

    @dispatchio_handler(job_name="ingest")
    def _worker(run_key: str, job_name: str) -> None:
        assert run_key == "20260418"
        assert job_name == "ingest"

    response = _worker({"run_key": "20260418"}, None)

    assert response["status"] == "ok"
    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].correlation_id == correlation_id
    assert events[0].status == Status.DONE

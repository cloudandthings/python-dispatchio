from __future__ import annotations

from uuid import uuid4

import boto3
from moto import mock_aws

from dispatchio.models import Status
from dispatchio.worker.harness import run_job
from dispatchio_aws.receiver.sqs import SQSReceiver
from dispatchio_aws.reporter.sqs import SQSReporter


@mock_aws
def test_report_posts_event_to_sqs() -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    correlation_id = uuid4()
    reporter = SQSReporter(queue_url=queue_url, region="eu-west-1")
    reporter.report(
        correlation_id=correlation_id,
        status=Status.DONE,
        metadata={"rows": 42},
    )

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].status == Status.DONE
    assert events[0].metadata["rows"] == 42
    assert events[0].correlation_id == correlation_id


@mock_aws
def test_run_job_auto_detects_sqs_reporter_from_env(monkeypatch) -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "sqs")
    monkeypatch.setenv("DISPATCHIO_RECEIVER__QUEUE_URL", queue_url)
    monkeypatch.setenv("DISPATCHIO_RECEIVER__REGION", "eu-west-1")
    correlation_id = uuid4()
    monkeypatch.setenv("DISPATCHIO_CORRELATION_ID", str(correlation_id))

    def _work(run_key: str) -> None:
        assert run_key == "20260414"

    run_job("ingest", _work, run_key="20260414", reporter=None)

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].correlation_id == correlation_id
    assert events[0].status == Status.DONE

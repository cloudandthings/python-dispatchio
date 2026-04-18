"""Optional AWS extension package for dispatchio."""

from dispatchio_aws.config import aws_orchestrator_from_config
from dispatchio_aws.receiver.sqs import SQSReceiver
from dispatchio_aws.reporter.sqs import SQSReporter

__all__ = ["SQSReceiver", "SQSReporter", "aws_orchestrator_from_config"]

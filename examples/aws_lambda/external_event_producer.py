"""Publish external dependency events to the configured receiver backend.

For SQS backends, set:
  DISPATCHIO_RECEIVER__BACKEND=sqs
  DISPATCHIO_RECEIVER__QUEUE_URL=...
  DISPATCHIO_RECEIVER__REGION=...

Usage:
  python examples/aws_lambda/external_event_producer.py 20250115
"""

from __future__ import annotations

import argparse

from dispatchio.reporter import report_external_done


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_key", help="Run key in the expected cadence format")
    args = parser.parse_args()

    report_external_done(
        event_name="event.user_registered",
        run_key=args.run_key,
        metadata={"source": "identity-service"},
    )
    report_external_done(
        event_name="event.kyc_passed",
        run_key=args.run_key,
        metadata={"source": "compliance-service"},
    )


if __name__ == "__main__":
    main()

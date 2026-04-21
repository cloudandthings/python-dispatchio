"""Worker functions for the multi-orchestrator example."""

import time


def ingest(run_key: str) -> None:
    """Simulate daily data ingestion."""
    time.sleep(0.1)
    print(f"Ingested daily records for {run_key}.")


def transform(run_key: str) -> None:
    """Simulate daily transformation — depends on ingest."""
    time.sleep(0.1)
    print(f"Transformed daily records for {run_key}.")


def aggregate(run_key: str) -> None:
    """Simulate weekly aggregation."""
    time.sleep(0.1)
    print(f"Aggregated weekly records for {run_key}.")


def report(run_key: str) -> None:
    """Simulate weekly report generation — depends on aggregate."""
    time.sleep(0.1)
    print(f"Generated weekly report for {run_key}.")

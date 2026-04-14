"""Worker functions for the multi-orchestrator example."""

import time


def ingest(run_id: str) -> None:
    """Simulate daily data ingestion."""
    time.sleep(0.1)
    print(f"Ingested daily records for {run_id}.")


def transform(run_id: str) -> None:
    """Simulate daily transformation — depends on ingest."""
    time.sleep(0.1)
    print(f"Transformed daily records for {run_id}.")


def aggregate(run_id: str) -> None:
    """Simulate weekly aggregation."""
    time.sleep(0.1)
    print(f"Aggregated weekly records for {run_id}.")


def report(run_id: str) -> None:
    """Simulate weekly report generation — depends on aggregate."""
    time.sleep(0.1)
    print(f"Generated weekly report for {run_id}.")

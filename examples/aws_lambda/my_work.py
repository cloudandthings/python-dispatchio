from __future__ import annotations

from dispatchio_aws.worker.lambda_handler import dispatchio_handler


@dispatchio_handler(job_name="ingest")
def handler(run_key: str) -> None:
    print(f"ingest lambda processing run_key={run_key}")

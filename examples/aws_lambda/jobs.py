from __future__ import annotations

import os
from pathlib import Path

from dispatchio import Job, LambdaJob
from dispatchio_aws.config import aws_orchestrator_from_config

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

JOBS = [
    Job.create(
        "ingest",
        executor=LambdaJob(
            function_name=os.getenv(
                "DISPATCHIO_LAMBDA_FUNCTION_NAME", "dispatchio-ingest"
            ),
            payload_template={"run_key": "{run_key}", "job_name": "{job_name}"},
        ),
    ),
]

orchestrator = aws_orchestrator_from_config(JOBS, config=CONFIG_FILE)

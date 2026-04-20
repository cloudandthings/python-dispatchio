"""Custom pool example.

Shows how to assign jobs to named admission pools and configure per-pool
limits in dispatchio.toml.
"""

from __future__ import annotations

import os
from pathlib import Path

from dispatchio import Job, PythonJob, orchestrator_from_config

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

JOBS = [
    Job.create(
        "replay_high",
        executor=PythonJob(script=str(BASE / "my_work.py"), function="replay_high"),
        pool="replay",
        priority=20,
    ),
    Job.create(
        "replay_low",
        executor=PythonJob(script=str(BASE / "my_work.py"), function="replay_low"),
        pool="replay",
        priority=10,
    ),
    Job.create(
        "bulk_high",
        executor=PythonJob(script=str(BASE / "my_work.py"), function="bulk_high"),
        pool="bulk",
        priority=5,
    ),
]

orchestrator = orchestrator_from_config(JOBS, config=CONFIG_FILE)

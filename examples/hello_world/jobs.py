"""
Hello World — Two Jobs and a Dependency.

Two jobs:
  1. hello_world   — runs immediately, prints a greeting.
  2. goodbye_world — runs after hello_world is done for the same day.

The Job class creates two jobs, and a dependency between them.

Configuration is loaded from dispatchio.toml in this directory.
For example, default_cadence is set to DAILY so it doesn't have to be specified in the Job definitions.

Run with:
  python examples/hello_world/run.py
"""

import os
from pathlib import Path

from dispatchio import Job, PythonJob, orchestrator_from_config

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

hello_world = Job.create(
    "hello_world",
    # default_cadence is set to DAILY in dispatchio.toml
    # cadence=DAILY,
    executor=PythonJob(
        script=str(BASE / "my_work.py"),
        function="hello_world",
    ),
)

goodbye_world = Job.create(
    name="goodbye_world",
    executor=PythonJob(
        script=str(BASE / "my_work.py"),
        function="goodbye_world",
    ),
    depends_on=hello_world,
    # default_cadence is set to DAILY in dispatchio.toml
    # cadence=DAILY,
)

JOBS = [hello_world, goodbye_world]
orchestrator = orchestrator_from_config(JOBS, config=CONFIG_FILE)

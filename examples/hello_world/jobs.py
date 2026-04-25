"""
Hello World — Explicit Job and Orchestrator definitions.

This is the explicit approach: Job objects are defined here and wired into
an Orchestrator.  Use this pattern for complex pipelines with cross-file
dependencies, dynamic registration, or graph loading.

For simple cases, see examples/hello_world/my_work.py which uses the
@job decorator instead.

Run with:
  python examples/hello_world/run.py
"""

import os
import logging
from pathlib import Path

from dispatchio import Job, PythonJob, orchestrator

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
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    # Run a single tick in the orchestrator, for demo purposes.
    # To run several ticks in a loop, see examples/hello_world/run.py which calls run_loop().
    orchestrator.tick()

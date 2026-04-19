"""Job definitions for the data_store example.

Demonstrates the DataStore pattern:
  1. discover — writes a list of entities to the DataStore.
  2. process  — waits for discover to complete, then reads the entity list.

The DataStore is configured in dispatchio.toml ([dispatchio.data_store]).
The orchestrator injects DISPATCHIO_DATA_DIR and DISPATCHIO_DATA_NAMESPACE into
worker subprocesses automatically, so workers call get_data_store() with no
extra configuration.
"""

import os
from pathlib import Path

from dispatchio import DAILY, Dependency, Job, PythonJob, orchestrator_from_config

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

discover = Job(
    name="discover",
    cadence=DAILY,
    executor=PythonJob(
        script=str(BASE / "workers.py"),
        function="discover",
    ),
)

process = Job(
    name="process",
    cadence=DAILY,
    executor=PythonJob(
        script=str(BASE / "workers.py"),
        function="process",
    ),
    depends_on=[Dependency(job_name="discover", cadence=DAILY)],
)

orchestrator = orchestrator_from_config([discover, process], config=CONFIG_FILE)

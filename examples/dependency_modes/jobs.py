"""
Dependency Modes example.

Demonstrates ALL_FINISHED and THRESHOLD dependency modes for downstream jobs.

Jobs in this example:
  entity_a, entity_b, entity_c  — three daily entity-processing jobs (no deps)

  best_effort_collector  — depends on all three entity jobs, dependency_mode=ALL_FINISHED
    Runs once all entity jobs have finished, regardless of success or failure.
    Useful when you want to process whatever data is available.

  majority_collector  — depends on all three entity jobs, dependency_mode=THRESHOLD,
                        dependency_threshold=2
    Runs as soon as 2 of 3 entity jobs have succeeded.
    Useful when partial success is sufficient to proceed.

The run.py script seeds the state store so that entity_a=DONE, entity_b=DONE,
entity_c=ERROR (simulating a partial success scenario), then calls simulate():
  - majority_collector: threshold=2 met (2/3 succeeded) → SUBMITTED
  - best_effort_collector: all entities are in a finished state → SUBMITTED

Run with:
  python examples/dependency_modes/run.py
"""

import os
from pathlib import Path

from dispatchio import (
    Dependency,
    DependencyMode,
    Job,
    PythonJob,
    orchestrator_from_config,
)

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

# Three independent entity-processing jobs.
entity_a = Job.create(
    "entity_a",
    PythonJob(script=str(BASE / "my_work.py"), function="entity_a"),
)

entity_b = Job.create(
    "entity_b",
    PythonJob(script=str(BASE / "my_work.py"), function="entity_b"),
)

entity_c = Job.create(
    "entity_c",
    PythonJob(script=str(BASE / "my_work.py"), function="entity_c"),
)

# Runs once all entity jobs have finished — regardless of success or failure.
# ALL_FINISHED proceeds as soon as every dep is in a finished state
# (DONE, ERROR, LOST, or SKIPPED).
best_effort_collector = Job.create(
    "best_effort_collector",
    PythonJob(script=str(BASE / "my_work.py"), function="best_effort_collector"),
    depends_on=[
        entity_a,
        entity_b,
        entity_c,
    ],
    dependency_mode=DependencyMode.ALL_FINISHED,
)

# Runs as soon as 2 of 3 entity jobs have succeeded.
# THRESHOLD proceeds once ≥ dependency_threshold deps reach required_status (DONE).
majority_collector = Job.create(
    "majority_collector",
    PythonJob(script=str(BASE / "my_work.py"), function="majority_collector"),
    depends_on=[
        entity_a,
        entity_b,
        entity_c,
    ],
    dependency_mode=DependencyMode.THRESHOLD,
    dependency_threshold=2,
)

JOBS = [entity_a, entity_b, entity_c, best_effort_collector, majority_collector]
orchestrator = orchestrator_from_config(JOBS, config=CONFIG_FILE)

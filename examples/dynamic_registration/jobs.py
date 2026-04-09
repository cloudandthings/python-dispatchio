"""
Dynamic registration example.

This example shows an orchestrator-first flow:
  1. Build the orchestrator from config.
  2. Register jobs with add_jobs().
  3. Optionally add more jobs after ticks have already run.
"""

from __future__ import annotations

import os
from pathlib import Path

from dispatchio import DAILY, Dependency, Job, PythonJob, orchestrator_from_config

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

orchestrator = orchestrator_from_config(
    config=CONFIG_FILE,
    allow_runtime_mutation=True,
)


def register_bootstrap_jobs() -> None:
    """Register the initial pipeline jobs once."""
    existing = {job.name for job in orchestrator.jobs}
    if "discover" in existing and "transform" in existing:
        return

    discover_job = Job.create(
        "discover",
        PythonJob(script=str(BASE / "my_work.py"), function="discover"),
        cadence=DAILY,
    )
    transform_job = Job.create(
        "transform",
        PythonJob(script=str(BASE / "my_work.py"), function="transform"),
        cadence=DAILY,
        depends_on=[Dependency(job_name="discover", cadence=DAILY)],
    )
    orchestrator.add_jobs([discover_job, transform_job])


def register_entity_jobs(entities: list[str]) -> None:
    """Register one job per entity after discovery completes."""
    existing = {job.name for job in orchestrator.jobs}
    new_jobs: list[Job] = []

    for entity in entities:
        job_name = f"process_entity_{entity}"
        if job_name in existing:
            continue
        new_jobs.append(
            Job.create(
                job_name,
                PythonJob(script=str(BASE / "my_work.py"), function=job_name),
                cadence=DAILY,
                depends_on=[Dependency(job_name="transform", cadence=DAILY)],
            )
        )

    if new_jobs:
        orchestrator.add_jobs(new_jobs)

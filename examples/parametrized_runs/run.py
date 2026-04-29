"""
Parametrized Runs example runner.

Demonstrates RunSpec / RunContext / DateContext with a PythonJob that
simulates a month-to-date pattern from jobs.py.

Normal mode:
  - Submits a "current_month" run from month-start to today.
  - On days 1-5 of a new month, also submits a "previous_month" run.

Run:
    python examples/parametrized_runs/run.py
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import (
    DAILY,
    Job,
    PythonJob,
    RunContext,
    RunSpec
)
from dispatchio.config import orchestrator
from dispatchio.run_loop import run_loop

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def date_range_runs(rc: RunContext) -> list[RunSpec]:
    runs = [
        RunSpec(
            variant="current_month",
            params={
                "start_date": rc.dates["mon0_first_yyyymmddD"],
                "end_date": rc.dates["day0_yyyymmddD"],
            },
        )
    ]
    # Overlap window: if today and 5 days ago are in different months,
    # also run a full previous-month query to catch late-arriving data.
    if rc.dates["day0_yyyymm"] != rc.dates["day5_yyyymm"]:
        runs.append(
            RunSpec(
                variant="previous_month",
                params={
                    "start_date": rc.dates["mon1_first_yyyymmddD"],
                    "end_date": rc.dates["mon1_last_yyyymmddD"],
                },
            )
        )
    return runs


process_date_range = Job.create(
    "process_date_range",
    PythonJob(
        script=str(BASE / "my_work.py"),
        function="process",
    ),
    cadence=DAILY,
    runs=date_range_runs,
)

JOBS = [process_date_range]
_orchestrator = orchestrator(JOBS, config=CONFIG_FILE)

if __name__ == "__main__":
    run_loop(_orchestrator)

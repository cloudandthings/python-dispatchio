"""
Parametrized Runs example worker.

Reads date-range parameters from DISPATCHIO_PARAM_* environment variables
injected by the orchestrator at submit time, then reports completion via
the Dispatchio harness.

In the real world, this function would do some useful work.
Here we just print and exit.
"""

import os


def process(run_key: str) -> None:
    start_date = os.environ.get("DISPATCHIO_PARAM_START_DATE", "<not set>")
    end_date = os.environ.get("DISPATCHIO_PARAM_END_DATE", "<not set>")
    print(f"[{run_key}] Processing date range: {start_date} → {end_date}")

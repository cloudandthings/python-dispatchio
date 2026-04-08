"""
Dependency Modes demo runner.

Seeds the state store to simulate a partial success scenario:
  entity_a = DONE   (succeeded)
  entity_b = DONE   (succeeded)
  entity_c = ERROR  (failed)

Then calls simulate() with reference_time=2025-01-15 09:00 UTC.

Expected outcome:
  majority_collector   — SUBMITTED  (threshold=2 met: 2/3 succeeded)
  best_effort_collector — SUBMITTED  (ALL_FINISHED: all entities are finished)

Run with:
  python examples/dependency_modes/run.py
"""

import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import RunRecord, Status, simulate
from examples.dependency_modes.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

REF = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
RUN_ID = REF.strftime("%Y%m%d")  # "20250115"

# Seed the state store to simulate entity results without running the jobs.
orchestrator.state.put(
    RunRecord(job_name="entity_a", run_id=RUN_ID, status=Status.DONE)
)
orchestrator.state.put(
    RunRecord(job_name="entity_b", run_id=RUN_ID, status=Status.DONE)
)
orchestrator.state.put(
    RunRecord(
        job_name="entity_c",
        run_id=RUN_ID,
        status=Status.ERROR,
        error_reason="simulated failure",
    )
)

simulate(
    orchestrator,
    reference_time=REF,
)

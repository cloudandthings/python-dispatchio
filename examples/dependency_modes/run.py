"""
Dependency Modes demo runner.

Seeds the state store to simulate a partial success scenario:
  entity_a = DONE   (succeeded)
  entity_b = DONE   (succeeded)
  entity_c = ERROR  (failed)

Then calls run_loop() with reference_time=2025-01-15 09:00 UTC.

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

from uuid import uuid4
from dispatchio.models import AttemptRecord, TriggerType
from dispatchio import Status, run_loop
from examples.dependency_modes.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

REF = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
RUN_KEY = REF.strftime("%Y%m%d")  # "20250115"


# Seed the state store to simulate entity results without running the jobs.
def _seed(job_name: str, status: Status, reason: str | None = None) -> None:
    orchestrator.state.append_attempt(
        AttemptRecord(
            job_name=job_name,
            run_key=RUN_KEY,
            attempt=0,
            correlation_id=uuid4(),
            status=status,
            reason=reason,
            trigger_type=TriggerType.SCHEDULED,
            trace={},
        )
    )


_seed("entity_a", Status.DONE)
_seed("entity_b", Status.DONE)
_seed("entity_c", Status.ERROR, reason="simulated failure")

run_loop(
    orchestrator,
    reference_time=REF,
)

print("\nAttempt history with trigger metadata:")
for attempt_record in orchestrator.state.list_attempts(run_key=RUN_KEY):
    print(
        f"- {attempt_record.job_name} "
        f"attempt={attempt_record.attempt} "
        f"status={attempt_record.status.value} "
        f"reason={attempt_record.reason} "
        f"trigger={attempt_record.trigger_type.value} "
        f"trigger_reason={attempt_record.trigger_reason}"
    )

"""
Schedule Conditions demo runner.

Uses reference_time=2025-01-15 18:30 UTC (Wednesday evening) so that all
conditions in the example are satisfied and every job completes cleanly.

To see conditions block jobs, try:
  datetime(2025, 1, 15,  7, 0, ...)   # 07:00 — morning_report + after_hours_batch blocked
  datetime(2025, 1, 18, 18, 30, ...)  # Saturday — weekday_digest + after_hours_batch blocked

Run with:
  python examples/conditions/run.py
"""

import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio.run_loop import run_loop
from examples.conditions.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

run_loop(
    orchestrator,
    reference_time=datetime(2025, 1, 15, 18, 30, tzinfo=timezone.utc),
)

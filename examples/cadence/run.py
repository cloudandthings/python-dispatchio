"""
Cadence & Cross-Cadence Dependencies demo runner.

Uses reference_time=2025-01-15 09:00 UTC (Wednesday, mid-month) so that:
  monthly_ledger  → run_id = "202501"   (submits immediately)
  daily_reconcile → run_id = "20250115" (blocked until monthly_ledger/202501 done)
  weekly_summary  → run_id = "20250113" (blocked until monthly_ledger/202501 done)
  yesterday_load  → run_id = "20250114" (submits immediately, independent)

Run with:
  python examples/cadence/run.py
"""

import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import simulate
from examples.cadence.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

simulate(
    orchestrator,
    reference_time=datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc),
)

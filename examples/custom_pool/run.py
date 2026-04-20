"""Custom pool demo runner.

Run with:
  python examples/custom_pool/run.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from examples.custom_pool.jobs import orchestrator  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


if __name__ == "__main__":
    ref = datetime.now(tz=timezone.utc)

    log.info("Dry-run tick: replay pool capped at 1 submit per tick")
    result = orchestrator.tick(reference_time=ref, dry_run=True)
    for event in result.results:
        suffix = f" ({event.detail})" if event.detail else ""
        log.info(
            "  pool=%s  %s[%s] -> %s%s",
            event.pool,
            event.job_name,
            event.run_id,
            event.action.value,
            suffix,
        )

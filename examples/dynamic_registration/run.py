"""
Dynamic registration demo runner.

Run with:
  python examples/dynamic_registration/run.py
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from examples.dynamic_registration.jobs import (  # noqa: E402
    orchestrator,
    register_bootstrap_jobs,
    register_entity_jobs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


if __name__ == "__main__":
    reference_time = datetime.now(tz=timezone.utc)

    register_bootstrap_jobs()

    for tick_num in range(1, 8):
        if tick_num == 3:
            register_entity_jobs(["alpha", "beta"])
            log.info("Registered dynamic entity jobs after initial ticks")

        result = orchestrator.tick(reference_time=reference_time)
        log.info("Tick %d", tick_num)
        for event in result.results:
            suffix = f" ({event.detail})" if event.detail else ""
            log.info(
                "  %s[%s] -> %s%s",
                event.job_name,
                event.run_id,
                event.action.value,
                suffix,
            )
        time.sleep(0.3)

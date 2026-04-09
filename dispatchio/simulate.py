"""
Local development simulator — drives an Orchestrator through multiple ticks
in a loop with a real sleep between them.

This is NOT how Dispatchio runs in production. A production deployment calls
exactly one tick per scheduler invocation — an EventBridge rule triggering
a Lambda, a cron job on EC2, a Kubernetes CronJob, etc.:

    orchestrator.tick()

The simulator exists only for local development and demos, where you want
to watch a full run play out without wiring up a real scheduler.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dispatchio.orchestrator import Orchestrator
    from dispatchio.state.base import StateStore
    from dispatchio.models import Job

log = logging.getLogger(__name__)


def simulate(
    orchestrator: Orchestrator,
    *,
    tick_interval: float = 2.0,
    max_ticks: int = 20,
    stop_when: Callable[[StateStore, list[Job], str], bool] | None = None,
    reference_time: datetime | None = None,
) -> None:
    """
    Tick the orchestrator in a loop until a stop condition is met.

    All ticks share the same reference_time (locked at call time, or supplied
    explicitly) so they operate on the same logical "day" even as real-clock
    time advances between sleeps.

    Args:
        orchestrator:   The Orchestrator to drive.
        tick_interval:  Seconds to sleep between ticks.
        max_ticks:      Hard upper bound on the number of ticks.
        stop_when:      Callable(store, jobs, run_id) -> bool. Called after
                        each tick; the loop exits when it returns True.
                        Defaults to stopping once every job is in a finished
                        state (DONE, ERROR, LOST, or SKIPPED).
        reference_time: Logical "now" used for every tick. Defaults to the
                        moment simulate() is called.
    """
    if reference_time is None:
        reference_time = datetime.now(tz=timezone.utc)
    if stop_when is None:
        stop_when = _all_finished

    run_id = reference_time.strftime("%Y%m%d")

    for tick_num in range(1, max_ticks + 1):
        log.info(
            "─── Tick %d (reference=%s) ───",
            tick_num,
            reference_time.strftime("%H:%M:%S"),
        )
        result = orchestrator.tick(reference_time=reference_time)

        for r in result.results:
            log.info(
                "  %-25s [%s] → %s%s",
                r.job_name,
                r.run_id,
                r.action.value,
                f"  ({r.detail})" if r.detail else "",
            )

        if stop_when(orchestrator.state, orchestrator.jobs, run_id):
            log.info("Stop condition met. Final state:")
            for j in orchestrator.jobs:
                rec = orchestrator.state.get(j.name, run_id)
                status = rec.status.value if rec else "NO_RECORD"
                suffix = f"  — {rec.error_reason}" if rec and rec.error_reason else ""
                log.info("  %-25s %s%s", j.name, status, suffix)
            return

        if tick_num < max_ticks:
            time.sleep(tick_interval)

    log.warning("Reached max_ticks=%d without the stop condition being met.", max_ticks)


def _all_finished(store: StateStore, jobs: list[Job], run_id: str) -> bool:
    """Default stop condition: every job has a finished record for run_id."""
    return all((rec := store.get(j.name, run_id)) and rec.is_finished() for j in jobs)

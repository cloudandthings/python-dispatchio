"""
Subprocess example demo runner.

Runs the orchestrator through multiple ticks, demonstrating a SubprocessJob
that succeeds and one that fails with retries.

    python examples/subprocess_example/run.py

In production, replace simulate() with a single tick() call triggered by
your scheduler (EventBridge, cron, etc.):

    orchestrator.tick()
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import simulate
from examples.subprocess_example.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

simulate(orchestrator)

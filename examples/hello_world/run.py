"""
Hello World demo runner.

Runs the orchestrator through multiple ticks so you can watch the jobs
complete without setting up a real scheduler.

    python examples/hello_world/run.py

In production, replace simulate() with a single tick() call triggered by
your scheduler (EventBridge, cron, etc.):

    orchestrator.tick()
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import simulate
from examples.hello_world.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

simulate(orchestrator)

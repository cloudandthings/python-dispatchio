"""
Hello World demo runner.

Two ways to run this example:

  1. Decorator style (no jobs.py needed) — run-file discovers @job functions:

       dispatchio run-file examples/hello_world/my_work.py

     For an explicit run key:

       dispatchio run-file examples/hello_world/my_work.py --run-key D20260423

  2. Explicit orchestrator style (suitable for complex pipelines):

       python examples/hello_world/run.py

     This script uses jobs.py, which defines the same jobs using the Job/PythonJob
     API directly.  In production, replace run_loop() with a single tick() call
     triggered by your scheduler (EventBridge, cron, etc.).
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio.run_loop import run_loop
from examples.hello_world.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

run_loop(orchestrator)

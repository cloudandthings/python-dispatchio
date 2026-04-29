"""DataStore example runner.

Shows how the DataStore enables one job to pass structured data to another:
  1. discover runs first and writes a list of entities to the DataStore.
  2. process waits for discover to complete, reads the entity list, and
     processes each one.

    python examples/data_store/run.py

In production, replace run_loop() with a single tick() call:
    orchestrator.tick()
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio.run_loop import run_loop
from examples.data_store.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

run_loop(orchestrator)

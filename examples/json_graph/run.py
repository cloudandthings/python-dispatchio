"""JSON Graph example — load a graph artifact and run it.

Demonstrates the JSON authoring mode: the graph is defined entirely in
graph.json with no Python job definitions. The runner only wires infrastructure
(state store, receiver) via dispatchio.toml.

    # Validate the artifact first (optional but recommended in CI):
    dispatchio graph validate examples/json_graph/graph.json

    # Run:
    python examples/json_graph/run.py

In production, replace run_loop() with a single tick() call driven by your
scheduler (EventBridge, cron, etc.):
    orchestrator.tick()
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio.run_loop import run_loop
from dispatchio.graph import load_graph, orchestrator_from_graph

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = Path(__file__).parent

spec = load_graph(BASE / "graph.json")
orchestrator = orchestrator_from_graph(spec, config=BASE / "dispatchio.toml")

run_loop(orchestrator)

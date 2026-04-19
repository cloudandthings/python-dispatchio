"""dispatchio.graph — JSON graph artifact loading and validation.

Quick start:
    from dispatchio.graph import load_graph, orchestrator_from_graph

    spec = load_graph("graph.json")
    orchestrator = orchestrator_from_graph(spec, config="dispatchio.toml")
    orchestrator.tick()

To dump the JSON Schema for non-Python producers:
    from dispatchio.graph import dump_schema
    import json
    print(json.dumps(dump_schema(), indent=2))

    # Or via CLI:
    # dispatchio graph schema > graph-spec-v1.json
"""

from dispatchio.graph.spec import GraphExternalDependency, GraphSpec, ProducerInfo
from dispatchio.graph.loader import (
    GraphValidationError,
    dump_schema,
    load_graph,
    orchestrator_from_graph,
    validate_graph,
)

__all__ = [
    "GraphExternalDependency",
    "GraphSpec",
    "ProducerInfo",
    "GraphValidationError",
    "dump_schema",
    "load_graph",
    "orchestrator_from_graph",
    "validate_graph",
]

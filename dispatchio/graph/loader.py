"""Graph loading, validation, and orchestrator construction.

Public API:
    load_graph(path)              — parse + Pydantic validate a JSON file → GraphSpec
    validate_graph(spec)          — graph-level invariants; collects all errors before raising
    dump_schema()                 — GraphSpec.model_json_schema() for non-Python producers
    orchestrator_from_graph(...)  — full chain: validate → build Orchestrator
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dispatchio.graph.spec import GraphSpec
from dispatchio.models import EventDependency, JobDependency

if TYPE_CHECKING:
    from dispatchio.config.settings import DispatchioSettings
    from dispatchio.orchestrator import Orchestrator


class GraphValidationError(Exception):
    """Raised when a graph artifact fails validation.

    errors is a list of individual error strings so callers can inspect them
    programmatically. The str() representation lists all errors at once so
    producers can fix everything in one iteration.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        lines = [f"Graph validation failed ({len(errors)} error(s)):"]
        for i, msg in enumerate(errors, 1):
            lines.append(f"  {i}. {msg}")
        super().__init__("\n".join(lines))


# ---------------------------------------------------------------------------
# load_graph
# ---------------------------------------------------------------------------


def load_graph(path: str | Path) -> GraphSpec:
    """Parse and Pydantic-validate a graph artifact from a JSON file.

    Raises:
        FileNotFoundError: The file does not exist.
        GraphValidationError: The JSON is malformed or Pydantic validation fails.
            All field errors are collected and reported together.
    """
    p = Path(path)
    try:
        raw = json.loads(p.read_text())
    except json.JSONDecodeError as exc:
        raise GraphValidationError([f"Invalid JSON: {exc}"]) from exc

    try:
        return GraphSpec.model_validate(raw)
    except ValidationError as exc:
        errors = [
            f"{' → '.join(str(loc) for loc in err['loc'])}: {err['msg']}"
            for err in exc.errors()
        ]
        raise GraphValidationError(errors) from exc


# ---------------------------------------------------------------------------
# validate_graph
# ---------------------------------------------------------------------------


def validate_graph(spec: GraphSpec) -> None:
    """Check graph-level invariants not enforced by Pydantic field validation.

    Checks (all errors collected before raising):
      1. Duplicate job names
      2. Internal depends_on references that don't resolve to a job in the graph
      3. External depends_on references (event.*/external.*) not declared in
         external_dependencies
      4. Circular dependency chains

    Raises:
        GraphValidationError: listing every error found.
    """
    errors: list[str] = []

    # 1. Duplicate job names
    seen: set[str] = set()
    for job in spec.jobs:
        if job.name in seen:
            errors.append(f"Duplicate job name: {job.name!r}")
        seen.add(job.name)

    # 2 + 3. Dependency resolution
    job_names: set[str] = {j.name for j in spec.jobs}
    declared_external: set[str] = {e.name for e in spec.external_dependencies}

    for job in spec.jobs:
        for dep in job.depends_on:
            if isinstance(dep, EventDependency):
                if dep.event_name not in declared_external:
                    errors.append(
                        f"Job {job.name!r} depends on undeclared event "
                        f"{dep.event_name!r}. Add it to external_dependencies."
                    )
            else:
                assert isinstance(dep, JobDependency)
                is_external = dep.job_name.startswith(
                    "event."
                ) or dep.job_name.startswith("external.")
                if is_external:
                    if dep.job_name not in declared_external:
                        errors.append(
                            f"Job {job.name!r} depends on undeclared external "
                            f"{dep.job_name!r}. Add it to external_dependencies."
                        )
                elif dep.job_name not in job_names:
                    errors.append(
                        f"Job {job.name!r} depends on {dep.job_name!r} "
                        f"which is not defined in this graph."
                    )

    # 4. Circular dependency detection (internal deps only)
    internal_adj: dict[str, list[str]] = {j.name: [] for j in spec.jobs}
    for job in spec.jobs:
        for dep in job.depends_on:
            if isinstance(dep, JobDependency) and dep.job_name in job_names:
                internal_adj[job.name].append(dep.job_name)

    errors.extend(_find_cycles(internal_adj))

    if errors:
        raise GraphValidationError(errors)


def _find_cycles(adj: dict[str, list[str]]) -> list[str]:
    """Return error messages for every distinct cycle found via DFS."""
    UNVISITED, IN_PROGRESS, DONE = 0, 1, 2
    state: dict[str, int] = {n: UNVISITED for n in adj}
    errors: list[str] = []
    seen_cycles: set[frozenset[str]] = set()

    def dfs(node: str, path: list[str]) -> None:
        state[node] = IN_PROGRESS
        for dep in adj[node]:
            if state[dep] == IN_PROGRESS and dep in path:
                cycle_key = frozenset(path[path.index(dep) :])
                if cycle_key not in seen_cycles:
                    seen_cycles.add(cycle_key)
                    cycle_str = " → ".join(path[path.index(dep) :] + [dep])
                    errors.append(f"Circular dependency: {cycle_str}")
            elif state[dep] == UNVISITED:
                dfs(dep, path + [dep])
        state[node] = DONE

    for node in adj:
        if state[node] == UNVISITED:
            dfs(node, [node])

    return errors


# ---------------------------------------------------------------------------
# dump_schema
# ---------------------------------------------------------------------------


def dump_schema() -> dict:
    """Return the JSON Schema for GraphSpec.

    Use this to generate a schema file for non-Python producers:
        dispatchio graph schema > graph-spec-v1.json
    """
    return GraphSpec.model_json_schema()


# ---------------------------------------------------------------------------
# orchestrator_from_graph
# ---------------------------------------------------------------------------


def orchestrator_from_graph(
    spec: GraphSpec,
    config: str | Path | DispatchioSettings | None = None,
    **orchestrator_kwargs,
) -> Orchestrator:
    """Build a fully-wired Orchestrator from a GraphSpec.

    Calls validate_graph() before building. If the graph declares
    external_dependencies, strict_dependencies is set to False automatically
    (unless the caller explicitly passes it).

    The orchestrator is named from spec.name, overriding any name set in
    the config file.

    Args:
        spec:   A loaded GraphSpec (from load_graph or model_validate).
        config: Forwarded to orchestrator (path, settings object,
                or None for auto-discovery).
        **orchestrator_kwargs:
                Forwarded to Orchestrator (e.g. alert_handler=...).
    """
    from dispatchio.config.factory import orchestrator

    validate_graph(spec)

    if spec.external_dependencies and "strict_dependencies" not in orchestrator_kwargs:
        orchestrator_kwargs["strict_dependencies"] = False

    # Pass namespace via kwargs; orchestrator pops it before building
    # the Orchestrator so there is no duplicate-keyword conflict.
    orchestrator_kwargs["namespace"] = spec.name

    return orchestrator(
        jobs=spec.jobs,
        config=config,
        **orchestrator_kwargs,
    )

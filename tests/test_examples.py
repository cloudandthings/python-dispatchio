"""Smoke tests that execute example runners."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(frozen=True)
class ExampleCase:
    """Single runnable example entrypoint."""

    name: str
    script: Path


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES: tuple[ExampleCase, ...] = (
    ExampleCase("hello_world", REPO_ROOT / "examples" / "hello_world" / "run.py"),
    ExampleCase("cadence", REPO_ROOT / "examples" / "cadence" / "run.py"),
    ExampleCase("conditions", REPO_ROOT / "examples" / "conditions" / "run.py"),
    ExampleCase(
        "dependency_modes",
        REPO_ROOT / "examples" / "dependency_modes" / "run.py",
    ),
    ExampleCase(
        "dynamic_registration",
        REPO_ROOT / "examples" / "dynamic_registration" / "run.py",
    ),
    ExampleCase(
        "subprocess_example",
        REPO_ROOT / "examples" / "subprocess_example" / "run.py",
    ),
    ExampleCase(
        "multi_orchestrator",
        REPO_ROOT / "examples" / "multi_orchestrator" / "run.py",
    ),
)


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda e: e.name)
def test_example_runs(example: ExampleCase, tmp_path: Path) -> None:
    """Each example script should run to completion with exit code 0."""
    env = os.environ.copy()
    env["DISPATCHIO_STATE__BACKEND"] = "sqlalchemy"
    env["DISPATCHIO_STATE__CONNECTION_STRING"] = f"sqlite:///{tmp_path / 'dispatchio.db'}"
    env["DISPATCHIO_RECEIVER__DROP_DIR"] = str(tmp_path / "completions")
    env["DISPATCHIO_STATE__TICK_LOG_PATH"] = str(tmp_path / "tick_log.jsonl")
    env["DISPATCHIO_TICK_INTERVAL"] = "0.01"  # Fast ticks for tests

    completed = subprocess.run(
        [sys.executable, str(example.script)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 0, (
        f"example {example.name} failed\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )

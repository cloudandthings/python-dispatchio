"""
Subprocess example — demonstrates SubprocessJob.

Two jobs:
  1. generate  — runs a subprocess command, succeeds.
  2. summarize — depends on generate, always fails (shows retries + ERROR).

Key difference from PythonJob:
  SubprocessJob does not get the dispatchio harness automatically. The worker
  script must call run_job() itself, and the job's `env` config must inject
  DISPATCHIO_RUN_KEY and DISPATCHIO_DROP_DIR so run_job() can report completion
  back to the orchestrator.

  sys.executable is used instead of "python" so the subprocess inherits the
  same interpreter (and installed packages) as the orchestrator process —
  this keeps the example cross-platform without requiring "python" on PATH.

Run with:
    python examples/subprocess_example/run.py
"""

import os
import sys
from pathlib import Path

from dispatchio import Job, RetryPolicy, SubprocessJob
from dispatchio.config import orchestrator

BASE = Path(__file__).parent

# Must match receiver.drop_dir in dispatchio.toml.
# Injected into each subprocess so run_job() can auto-configure FilesystemReporter.
DROP_DIR = str(BASE / ".dispatchio" / "completions")

ENV = {"DISPATCHIO_RUN_KEY": "{run_key}", "DISPATCHIO_DROP_DIR": DROP_DIR}

generate = Job.create(
    "generate",
    SubprocessJob(
        command=[sys.executable, str(BASE / "my_work.py"), "generate"], env=ENV
    ),
)
summarize = Job.create(
    "summarize",
    SubprocessJob(
        command=[sys.executable, str(BASE / "my_work.py"), "summarize"], env=ENV
    ),
    depends_on=[generate],
    retry_policy=RetryPolicy(max_attempts=2),
)

JOBS = [generate, summarize]

CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)

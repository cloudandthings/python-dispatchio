"""
PythonJob executor.

Submits a PythonJob by spawning:
    {sys.executable} -m dispatchio run {entry_point}
    {sys.executable} -m dispatchio run-script {path} {name}

Run ID and reporter configuration are injected as environment variables so
the job harness (run_job) picks them up without any Dispatchio-specific argument
parsing in the job itself.

Environment variables injected at submit time:
    DISPATCHIO_RUN_KEY      the resolved run_key for this tick
    DISPATCHIO_CONFIG       optional global config location for worker-side loading
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

from dispatchio.executor.subprocess_ import _BaseSubprocessExecutor
from dispatchio.models import Attempt, Job, PythonJob

logger = logging.getLogger(__name__)


class PythonJobExecutor(_BaseSubprocessExecutor):
    """
    Executor for PythonJob configs.

    Spawns `python -m dispatchio run` or `python -m dispatchio run-script`
    as a detached subprocess. Run ID and reporter env vars are injected
    automatically; the job function itself stays free of Dispatchio imports.

    Args:
        env: Env vars that configure the subprocess. This now primarily includes
             DISPATCHIO_CONFIG so workers can load receiver settings from shared config.
    """

    def submit(
        self,
        job: Job,
        attempt: Attempt,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        cfg = job.executor
        if not isinstance(cfg, PythonJob):
            raise TypeError(
                f"PythonJobExecutor requires PythonJob, got {type(cfg).__name__}"
            )

        if cfg.entry_point is not None:
            cmd = [sys.executable, "-m", "dispatchio", "run", cfg.entry_point]
        else:
            cmd = [
                sys.executable,
                "-m",
                "dispatchio",
                "run-script",
                cfg.script,
                cfg.function,
            ]

        env = {
            **self._env,
            "DISPATCHIO_JOB_NAME": job.name,
            "DISPATCHIO_RUN_KEY": attempt.run_key,
            "DISPATCHIO_ATTEMPT": str(attempt.attempt),
            "DISPATCHIO_CORRELATION_ID": str(attempt.correlation_id),
        }

        for k, v in attempt.params.items():
            env[f"DISPATCHIO_PARAM_{k.upper()}"] = str(v)

        if cfg.pythonpath:
            extra = os.pathsep.join(str(p) for p in cfg.pythonpath)
            existing = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = f"{extra}{os.pathsep}{existing}" if existing else extra

        self._spawn(cmd, env, attempt)

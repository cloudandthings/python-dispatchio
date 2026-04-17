"""
PythonJob executor.

Submits a PythonJob by spawning:
    {sys.executable} -m dispatchio run {entry_point}
    {sys.executable} -m dispatchio run --script {path} --function {name}

Run ID and reporter configuration are injected as environment variables so
the job harness (run_job) picks them up without any Dispatchio-specific argument
parsing in the job itself.

Environment variables injected at submit time:
    DISPATCHIO_RUN_ID          the resolved run_id for this tick
    DISPATCHIO_DROP_DIR        filesystem completions dir (when receiver is filesystem)
    ... any other reporter_env entries (e.g. SQS queue URL)
    PYTHONPATH              prepended with PythonJob.pythonpath entries (if any)
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime

from dispatchio.models import Job, PythonJob, RunRecord, Status


class PythonJobExecutor:
    """
    Executor for PythonJob configs.

    Spawns `python -m dispatchio run` as a detached subprocess. Run ID and
    reporter env vars are injected automatically; the job function itself
    stays free of Dispatchio imports.

    Implements Pokeable: poke() checks subprocess liveness via poll() so the
    orchestrator can detect crashed jobs even without a heartbeat policy.

    Args:
        reporter_env: Env vars that configure the reporter inside the
                      spawned subprocess. Built by orchestrator_from_config
                      from the receiver settings (e.g. DISPATCHIO_DROP_DIR
                      for FilesystemReceiver, SQS vars for SQSReceiver).
    """

    def __init__(self, reporter_env: dict[str, str] | None = None) -> None:
        self._reporter_env: dict[str, str] = reporter_env or {}
        self._processes: dict[tuple[str, str], subprocess.Popen] = {}

    def submit(
        self,
        job: Job,
        run_id: str,
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
                "run",
                "--script",
                cfg.script,
                "--function",
                cfg.function,
            ]

        env = {**os.environ, **self._reporter_env, "DISPATCHIO_RUN_ID": run_id}

        if cfg.pythonpath:
            extra = os.pathsep.join(str(p) for p in cfg.pythonpath)
            existing = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = f"{extra}{os.pathsep}{existing}" if existing else extra

        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        self._processes[(job.name, run_id)] = proc

    def poke(self, record: RunRecord) -> Status | None:
        """
        Check whether the spawned subprocess is still alive.

        Returns Status.RUNNING if the process is alive, Status.DONE if it
        exited cleanly (exit code 0), Status.ERROR if it exited with a
        non-zero code, or None if the process is not tracked (e.g. the
        orchestrator was restarted since submission).
        """
        proc = self._processes.get((record.job_name, record.run_id))
        if proc is None:
            return None
        returncode = proc.poll()
        if returncode is None:
            return Status.RUNNING
        del self._processes[(record.job_name, record.run_id)]
        return Status.DONE if returncode == 0 else Status.ERROR

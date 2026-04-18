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

import logging
import os
import subprocess
import sys
import time
from datetime import datetime

import psutil

from dispatchio.models import Job, PythonJob, RunRecord, Status

logger = logging.getLogger(__name__)


class PythonJobExecutor:
    """
    Executor for PythonJob configs.

    Spawns `python -m dispatchio run` as a detached subprocess. Run ID and
    reporter env vars are injected automatically; the job function itself
    stays free of Dispatchio imports.

    Implements Pokeable: poke() checks subprocess liveness via poll() so the
    orchestrator can detect crashed jobs via active polling.

    Args:
        reporter_env: Env vars that configure the reporter inside the
                      spawned subprocess. Built by orchestrator_from_config
                      from the receiver settings (e.g. DISPATCHIO_DROP_DIR
                      for FilesystemReceiver, SQS vars for SQSReceiver).
    """

    def __init__(self, reporter_env: dict[str, str] | None = None) -> None:
        self._reporter_env: dict[str, str] = reporter_env or {}
        self._processes: dict[tuple[str, str], subprocess.Popen] = {}
        self._references: dict[tuple[str, str], dict] = {}

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
        self._references[(job.name, run_id)] = {
            "pid": proc.pid,
            "start_time": time.time(),
        }

    def poke(self, record: RunRecord) -> Status | None:
        """
        Check whether the spawned subprocess is still alive.

        Returns Status.RUNNING if the process is alive, Status.DONE if it
        exited cleanly (exit code 0), Status.ERROR if it exited with a
        non-zero code, or None if the process cannot be determined.

        First checks in-memory process table (hot path). If not found and
        executor_reference is available (survived orchestrator restart),
        looks up the process by PID.
        """
        # Hot path: check in-memory process table
        proc = self._processes.get((record.job_name, record.run_id))
        if proc is not None:
            returncode = proc.poll()
            if returncode is None:
                return Status.RUNNING
            del self._processes[(record.job_name, record.run_id)]
            return Status.DONE if returncode == 0 else Status.ERROR

        # Cold path: lookup by PID from executor_reference (after restart)
        if not record.executor_reference:
            return None

        pid = record.executor_reference.get("pid")
        start_time = record.executor_reference.get("start_time")

        if pid is None:
            return None

        try:
            proc = psutil.Process(pid)

            # Verify this is the same process we started (prevent PID reuse issues)
            if start_time is not None:
                proc_start = proc.create_time()
                # Allow 1 second tolerance for floating point precision
                if abs(proc_start - start_time) > 1.0:
                    logger.warning(
                        "PID %d reused or stale for %s/%s (start time mismatch)",
                        pid,
                        record.job_name,
                        record.run_id,
                    )
                    return Status.ERROR

            returncode = proc.poll()
            if returncode is None:
                return Status.RUNNING
            return Status.DONE if returncode == 0 else Status.ERROR

        except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError):
            # Process doesn't exist or can't be accessed — it's dead
            return Status.ERROR

    def get_executor_reference(self, job_name: str, run_id: str) -> dict | None:
        """
        Retrieve the executor reference for a given job/run_id.
        Used by orchestrator to populate RunRecord.executor_reference.
        Returns {"pid": ..., "start_time": ...} or None if not tracked.
        """
        return self._references.get((job_name, run_id))

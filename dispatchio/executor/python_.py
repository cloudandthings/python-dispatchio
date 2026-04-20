"""
PythonJob executor.

Submits a PythonJob by spawning:
    {sys.executable} -m dispatchio run {entry_point}
    {sys.executable} -m dispatchio run-script {path} {name}

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
from uuid import UUID

import psutil

from dispatchio.models import AttemptRecord, Job, PythonJob, Status

logger = logging.getLogger(__name__)


class PythonJobExecutor:
    """
    Executor for PythonJob configs.

    Spawns `python -m dispatchio run` or `python -m dispatchio run-script`
    as a detached subprocess. Run ID and reporter env vars are injected
    automatically; the job function itself stays free of Dispatchio imports.

    Implements Pokeable: poke() checks subprocess liveness via poll() so the
    orchestrator can detect crashed jobs via active polling.

    Args:
        reporter_env: Env vars that configure the reporter inside the
                      spawned subprocess. Built by orchestrator_from_config
                      from the receiver settings (e.g. DISPATCHIO_DROP_DIR
                      for FilesystemReceiver, SQS vars for SQSReceiver).
    """

    def __init__(
        self,
        reporter_env: dict[str, str] | None = None,
        data_env: dict[str, str] | None = None,
    ) -> None:
        self._reporter_env: dict[str, str] = reporter_env or {}
        self._data_env: dict[str, str] = data_env or {}
        self._processes: dict[
            str, subprocess.Popen
        ] = {}  # keyed by str(dispatchio_attempt_id)
        self._references: dict[str, dict] = {}  # keyed by str(dispatchio_attempt_id)

    def submit(
        self,
        job: Job,
        attempt: AttemptRecord,
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
            **os.environ,
            **self._reporter_env,
            **self._data_env,
            "DISPATCHIO_JOB_NAME": job.name,
            "DISPATCHIO_RUN_ID": attempt.logical_run_id,
            "DISPATCHIO_ATTEMPT": str(attempt.attempt),
            "DISPATCHIO_ATTEMPT_ID": str(attempt.dispatchio_attempt_id),
        }

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
        attempt_id_str = str(attempt.dispatchio_attempt_id)
        self._processes[attempt_id_str] = proc
        self._references[attempt_id_str] = {
            "pid": proc.pid,
            "start_time": time.time(),
        }

    def poke(self, record: AttemptRecord) -> Status | None:
        """
        Check whether the spawned subprocess is still alive.

        Returns Status.RUNNING if the process is alive, Status.DONE if it
        exited cleanly (exit code 0), Status.ERROR if it exited with a
        non-zero code, or None if the process cannot be determined.

        First checks in-memory process table (hot path). If not found and
        trace.executor is available (survived orchestrator restart),
        looks up the process by PID.
        """
        attempt_id_str = str(record.dispatchio_attempt_id)
        # Hot path: check in-memory process table
        proc = self._processes.get(attempt_id_str)
        if proc is not None:
            returncode = proc.poll()
            if returncode is None:
                return Status.RUNNING
            del self._processes[attempt_id_str]
            return Status.DONE if returncode == 0 else Status.ERROR

        # Cold path: lookup by PID from trace.executor (after restart)
        executor_trace = record.trace.get("executor", {})
        if not executor_trace:
            return None

        pid = executor_trace.get("pid")
        start_time = executor_trace.get("start_time")

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
                        "PID %d reused or stale for %s/%s/%d (start time mismatch)",
                        pid,
                        record.job_name,
                        record.logical_run_id,
                        record.attempt,
                    )
                    return Status.ERROR

            returncode = proc.poll()
            if returncode is None:
                return Status.RUNNING
            return Status.DONE if returncode == 0 else Status.ERROR

        except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError):
            # Process doesn't exist or can't be accessed — it's dead
            return Status.ERROR

    def get_executor_reference(self, dispatchio_attempt_id: UUID) -> dict | None:
        """
        Retrieve the executor reference for an attempt UUID.
        Used by orchestrator to populate AttemptRecord.trace.executor.
        Returns {"pid": ..., "start_time": ...} or None if not tracked.
        """
        return self._references.get(str(dispatchio_attempt_id))

"""
Subprocess executor.

Launches the job as a child process using the command list defined in
SubprocessConfig. The process runs detached (via Popen without waiting),
so submit() returns immediately.

Template variables available in command strings and env values:
    {job_name}       — the job's name
    {run_id}         — the resolved run_id for this tick
    {reference_time} — ISO-8601 reference datetime string

Example config:
    SubprocessConfig(command=["python", "etl.py", "--date", "{run_id}"])
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from datetime import datetime
from uuid import UUID

import psutil

from dispatchio.models import AttemptRecord, Job, Status, SubprocessJob

logger = logging.getLogger(__name__)


class SubprocessExecutor:
    """
    Executor for SubprocessJob configs.

    Launches the job as a detached child process. The process is responsible
    for posting its own completion event (e.g. by calling run_job()).

    Implements Pokeable: poke() checks subprocess liveness via poll() so the
    orchestrator can detect crashed jobs via active polling.

    Args:
        data_env: Env vars that configure the DataStore inside spawned
                  subprocesses. Built from DataStore.worker_env() by the
                  orchestrator factory when a data_store is configured.
    """

    def __init__(self, data_env: dict[str, str] | None = None) -> None:
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
        if not isinstance(cfg, SubprocessJob):
            raise TypeError(
                f"SubprocessExecutor requires SubprocessJob, got {type(cfg).__name__}"
            )

        ctx = {
            "job_name": job.name,
            "run_id": attempt.logical_run_id,
            "logical_run_id": attempt.logical_run_id,
            "attempt": str(attempt.attempt),
            "reference_time": reference_time.isoformat(),
        }

        command = [part.format(**ctx) for part in cfg.command]

        env = {**os.environ, **self._data_env}
        for k, v in cfg.env.items():
            env[k] = v.format(**ctx)
        # Inject attempt identity for Phase 2 completion correlation
        env["DISPATCHIO_JOB_NAME"] = job.name
        env["DISPATCHIO_RUN_ID"] = attempt.logical_run_id
        env["DISPATCHIO_ATTEMPT"] = str(attempt.attempt)
        env["DISPATCHIO_ATTEMPT_ID"] = str(attempt.dispatchio_attempt_id)

        # Detach: we do not wait for completion.
        # The child process is responsible for posting a completion event.
        proc = subprocess.Popen(
            command,
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

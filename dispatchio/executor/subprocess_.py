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

import psutil

from dispatchio.models import Job, RunRecord, Status, SubprocessJob

logger = logging.getLogger(__name__)


class SubprocessExecutor:
    """
    Executor for SubprocessJob configs.

    Launches the job as a detached child process. The process is responsible
    for posting its own completion event (e.g. by calling run_job()).

    Implements Pokeable: poke() checks subprocess liveness via poll() so the
    orchestrator can detect crashed jobs via active polling.
    """

    def __init__(self) -> None:
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
        if not isinstance(cfg, SubprocessJob):
            raise TypeError(
                f"SubprocessExecutor requires SubprocessJob, got {type(cfg).__name__}"
            )

        ctx = {
            "job_name": job.name,
            "run_id": run_id,
            "reference_time": reference_time.isoformat(),
        }

        command = [part.format(**ctx) for part in cfg.command]

        env = {**os.environ}
        for k, v in cfg.env.items():
            env[k] = v.format(**ctx)

        # Detach: we do not wait for completion.
        # The child process is responsible for posting a completion event.
        proc = subprocess.Popen(
            command,
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

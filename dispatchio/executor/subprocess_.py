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

import os
import subprocess
from datetime import datetime

from dispatchio.models import Job, RunRecord, Status, SubprocessJob


class SubprocessExecutor:
    """
    Executor for SubprocessJob configs.

    Launches the job as a detached child process. The process is responsible
    for posting its own completion event (e.g. by calling run_job()).

    Implements Pokeable: poke() checks subprocess liveness via poll() so the
    orchestrator can detect crashed jobs even without a heartbeat policy.
    """

    def __init__(self) -> None:
        self._processes: dict[tuple[str, str], subprocess.Popen] = {}

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

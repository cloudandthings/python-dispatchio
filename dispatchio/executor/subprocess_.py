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

from dispatchio.models import Job, SubprocessJob


class SubprocessExecutor:

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
            "job_name":       job.name,
            "run_id":         run_id,
            "reference_time": reference_time.isoformat(),
        }

        command = [part.format(**ctx) for part in cfg.command]

        env = {**os.environ}
        for k, v in cfg.env.items():
            env[k] = v.format(**ctx)

        # Detach: we do not wait for completion.
        # The child process is responsible for posting a completion event.
        subprocess.Popen(
            command,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )

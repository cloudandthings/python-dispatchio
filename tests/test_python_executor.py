from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from dispatchio.executor.python_ import PythonJobExecutor
from dispatchio.models import AttemptRecord, Job, PythonJob, Status, TriggerType


def _make_attempt() -> AttemptRecord:
    return AttemptRecord(
        job_name="job_a",
        run_key="20250115",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.SUBMITTED,
        trigger_type=TriggerType.SCHEDULED,
        trace={},
    )


def _ref_time() -> datetime:
    return datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)


def test_python_executor_uses_run_for_entry_point(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs
            self.pid = 1234

    monkeypatch.setattr("dispatchio.executor.python_.subprocess.Popen", FakePopen)

    executor = PythonJobExecutor()
    job = Job(name="job_a", executor=PythonJob(entry_point="pkg.jobs:run_job"))

    executor.submit(job=job, attempt=_make_attempt(), reference_time=_ref_time())

    assert captured["cmd"] == [
        __import__("sys").executable,
        "-m",
        "dispatchio",
        "run",
        "pkg.jobs:run_job",
    ]


def test_python_executor_uses_run_script_for_script_form(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs
            self.pid = 5678

    monkeypatch.setattr("dispatchio.executor.python_.subprocess.Popen", FakePopen)

    executor = PythonJobExecutor()
    job = Job(
        name="job_a",
        executor=PythonJob(script="/tmp/worker.py", function="run_job"),
    )

    executor.submit(job=job, attempt=_make_attempt(), reference_time=_ref_time())

    assert captured["cmd"] == [
        __import__("sys").executable,
        "-m",
        "dispatchio",
        "run-script",
        "/tmp/worker.py",
        "run_job",
    ]

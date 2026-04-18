"""Tests for Pydantic model validation and behaviour."""

from datetime import datetime, time, timezone
import pytest
from dispatchio.cadence import DAILY, MONTHLY
from dispatchio.conditions import TimeOfDayCondition
from dispatchio.models import (
    Dependency,
    HttpJob,
    JobAction,
    Job,
    JobTickResult,
    PythonJob,
    AttemptRecord,
    Status,
    SubprocessJob,
    TickResult,
    TriggerType,
)


class TestStatus:
    def test_finished_statuses(self):
        assert Status.DONE in Status.finished()
        assert Status.ERROR in Status.finished()
        assert Status.LOST in Status.finished()
        assert Status.CANCELLED in Status.finished()

    def test_active_statuses(self):
        assert Status.SUBMITTED in Status.active()
        assert Status.QUEUED in Status.active()
        assert Status.RUNNING in Status.active()
        assert Status.DONE not in Status.active()

    def test_attempt_record_is_finished(self):
        from uuid import uuid4

        record = AttemptRecord(
            job_name="x",
            logical_run_id="1",
            attempt=0,
            dispatchio_attempt_id=uuid4(),
            status=Status.DONE,
        )
        assert record.is_finished()

    def test_attempt_record_is_active(self):
        from uuid import uuid4

        record = AttemptRecord(
            job_name="x",
            logical_run_id="1",
            attempt=0,
            dispatchio_attempt_id=uuid4(),
            status=Status.RUNNING,
        )
        assert record.is_active()
        assert not record.is_finished()


class TestAttemptRecord:
    def test_defaults(self):
        from uuid import uuid4

        r = AttemptRecord(
            job_name="job",
            logical_run_id="20250115",
            attempt=0,
            dispatchio_attempt_id=uuid4(),
            status=Status.SUBMITTED,
        )
        assert r.attempt == 0
        assert r.trace == {}
        assert r.reason is None
        assert r.trigger_type == TriggerType.SCHEDULED

    def test_model_copy_update(self):
        from uuid import uuid4

        attempt_id = uuid4()
        r = AttemptRecord(
            job_name="job",
            logical_run_id="20250115",
            attempt=0,
            dispatchio_attempt_id=attempt_id,
            status=Status.RUNNING,
        )
        updated = r.model_copy(update={"status": Status.DONE})
        assert updated.status == Status.DONE
        assert r.status == Status.RUNNING  # original unchanged


class TestSubprocessJob:
    def test_type_literal(self):
        cfg = SubprocessJob(command=["echo", "hello"])
        assert cfg.type == "subprocess"

    def test_discriminated_union_via_job(self):
        j = Job(
            name="j",
            cadence=DAILY,
            executor={"type": "subprocess", "command": ["echo"]},
        )
        assert isinstance(j.executor, SubprocessJob)


class TestHttpJob:
    def test_type_literal(self):
        cfg = HttpJob(url="http://example.com")
        assert cfg.type == "http"
        assert cfg.method == "POST"


class TestPythonJob:
    def test_entry_point_form(self):
        j = PythonJob(entry_point="mymodule:myfunc")
        assert j.entry_point == "mymodule:myfunc"
        assert j.script is None

    def test_script_form(self):
        j = PythonJob(script="/path/worker.py", function="run_etl")
        assert j.script == "/path/worker.py"
        assert j.function == "run_etl"

    def test_script_without_function_raises(self):
        with pytest.raises(Exception):
            PythonJob(script="/path/worker.py")

    def test_neither_form_raises(self):
        with pytest.raises(Exception):
            PythonJob()

    def test_discriminated_union_via_job(self):
        j = Job(
            name="j",
            cadence=DAILY,
            executor={"type": "python", "entry_point": "mod:fn"},
        )
        assert isinstance(j.executor, PythonJob)

    def test_pythonpath_default_empty(self):
        j = PythonJob(entry_point="mod:fn")
        assert j.pythonpath == []


class TestDependency:
    def test_monthly_cadence(self):
        dep = Dependency(job_name="up", cadence=MONTHLY)
        assert dep.cadence == MONTHLY


class TestJob:
    def test_defaults(self):
        j = Job(
            name="my_job",
            executor=SubprocessJob(command=["run.sh"]),
        )
        assert j.cadence is None  # inherits orchestrator default
        assert j.depends_on == []
        assert j.condition is None

    def test_dependency_list(self):
        j = Job(
            name="downstream",
            cadence=DAILY,
            executor=SubprocessJob(command=["run.sh"]),
            depends_on=[
                Dependency(job_name="upstream", cadence=DAILY),
                Dependency(job_name="other", cadence=MONTHLY),
            ],
        )
        assert len(j.depends_on) == 2

    def test_Job_shorthand_in_depends_on_list(self):
        upstream = Job(
            name="up",
            cadence=MONTHLY,
            executor=SubprocessJob(command=["x"]),
        )
        downstream = Job(
            name="down",
            cadence=DAILY,
            executor=SubprocessJob(command=["x"]),
            depends_on=[upstream],
        )
        assert downstream.depends_on[0].job_name == "up"
        assert downstream.depends_on[0].cadence == MONTHLY

    def test_Job_shorthand_single_item(self):
        """depends_on accepts a single Job without wrapping in a list."""
        upstream = Job(
            name="up",
            cadence=MONTHLY,
            executor=SubprocessJob(command=["x"]),
        )
        downstream = Job(
            name="down",
            cadence=DAILY,
            executor=SubprocessJob(command=["x"]),
            depends_on=upstream,
        )
        assert len(downstream.depends_on) == 1
        assert downstream.depends_on[0].job_name == "up"
        assert downstream.depends_on[0].cadence == MONTHLY

    def test_dependency_single_item(self):
        """depends_on accepts a single Dependency without wrapping in a list."""
        dep = Dependency(job_name="up", cadence=DAILY)
        downstream = Job(
            name="down",
            cadence=DAILY,
            executor=SubprocessJob(command=["x"]),
            depends_on=dep,
        )
        assert len(downstream.depends_on) == 1
        assert downstream.depends_on[0].job_name == "up"

    def test_Job_shorthand_requires_explicit_cadence(self):
        upstream = Job(
            name="up",
            executor=SubprocessJob(command=["x"]),
            cadence=None,
        )
        with pytest.raises(Exception, match="cadence=None"):
            Job(
                name="down",
                cadence=DAILY,
                executor=SubprocessJob(command=["x"]),
                depends_on=upstream,
            )

    def test_condition_accepts_time_of_day(self):
        j = Job(
            name="j",
            cadence=DAILY,
            executor=SubprocessJob(command=["x"]),
            condition=TimeOfDayCondition(after=time(9, 0)),
        )
        assert j.condition is not None

    def test_retry_policy_default_no_retries(self):
        j = Job(name="j", executor=SubprocessJob(command=["x"]))
        assert j.retry_policy.max_attempts == 1


class TestTickResult:
    def test_submitted_filter(self):
        ref = datetime(2025, 1, 15, tzinfo=timezone.utc)
        result = TickResult(
            reference_time=ref,
            results=[
                JobTickResult(job_name="a", run_id="x", action=JobAction.SUBMITTED),
                JobTickResult(
                    job_name="b", run_id="x", action=JobAction.SKIPPED_CONDITION
                ),
            ],
        )
        assert len(result.submitted()) == 1
        assert len(result.skipped()) == 1

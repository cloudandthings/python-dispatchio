"""Tests for Pydantic model validation and behaviour."""

from datetime import datetime, time, timezone
import pytest
from dispatchio.cadence import DAILY, MONTHLY
from dispatchio.conditions import TimeOfDayCondition
from dispatchio.models import (
    AlertCondition,
    AlertOn,
    Dependency,
    HeartbeatPolicy,
    HttpJob,
    JobAction,
    Job,
    JobTickResult,
    PythonJob,
    RetryPolicy,
    RunRecord,
    Status,
    SubprocessJob,
    TickResult,
)


class TestStatus:
    def test_finished_statuses(self):
        assert Status.DONE in Status.finished()
        assert Status.ERROR in Status.finished()
        assert Status.LOST in Status.finished()
        assert Status.SKIPPED in Status.finished()

    def test_active_statuses(self):
        assert Status.SUBMITTED in Status.active()
        assert Status.RUNNING in Status.active()
        assert Status.DONE not in Status.active()

    def test_run_record_is_finished(self):
        record = RunRecord(job_name="x", run_id="1", status=Status.DONE)
        assert record.is_finished()

    def test_run_record_is_active(self):
        record = RunRecord(job_name="x", run_id="1", status=Status.RUNNING)
        assert record.is_active()
        assert not record.is_finished()


class TestRunRecord:
    def test_defaults(self):
        r = RunRecord(job_name="job", run_id="20250115", status=Status.PENDING)
        assert r.attempt == 0
        assert r.metadata == {}
        assert r.error_reason is None

    def test_model_copy_update(self):
        r = RunRecord(job_name="job", run_id="20250115", status=Status.RUNNING)
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
        assert j.heartbeat is None
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

    def test_heartbeat_policy(self):
        j = Job(
            name="j",
            executor=SubprocessJob(command=["x"]),
            heartbeat=HeartbeatPolicy(timeout_seconds=600),
        )
        assert j.heartbeat.timeout_seconds == 600


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

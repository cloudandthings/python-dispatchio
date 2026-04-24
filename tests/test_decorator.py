"""Tests for the @job decorator and run-file CLI command."""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dispatchio import DAILY, MONTHLY
from dispatchio.cadence import LiteralCadence
from dispatchio.cli.app import app
from dispatchio.decorator import job
from dispatchio.models import Job, JobDependency, PythonJob


runner = CliRunner()


# ---------------------------------------------------------------------------
# @job decorator — unit tests
# ---------------------------------------------------------------------------


class TestJobDecoratorMetadata:
    def test_attaches_dispatchio_job(self):
        @job(cadence=DAILY)
        def my_fn(run_key: str) -> None:
            pass

        assert hasattr(my_fn, "_dispatchio_job")
        assert isinstance(my_fn._dispatchio_job, Job)

    def test_job_name_defaults_to_function_name(self):
        @job(cadence=DAILY)
        def extract(run_key: str) -> None:
            pass

        assert extract._dispatchio_job.name == "extract"

    def test_job_name_override(self):
        @job(cadence=DAILY, name="custom_name")
        def extract(run_key: str) -> None:
            pass

        assert extract._dispatchio_job.name == "custom_name"

    def test_cadence_is_set(self):
        @job(cadence=MONTHLY)
        def report(run_key: str) -> None:
            pass

        assert report._dispatchio_job.cadence == MONTHLY

    def test_no_cadence_is_none(self):
        @job()
        def bare(run_key: str) -> None:
            pass

        assert bare._dispatchio_job.cadence is None

    def test_executor_is_python_job(self):
        @job(cadence=DAILY)
        def worker(run_key: str) -> None:
            pass

        assert isinstance(worker._dispatchio_job.executor, PythonJob)
        assert worker._dispatchio_job.executor.function == "worker"

    def test_executor_script_points_to_this_file(self):
        @job(cadence=DAILY)
        def worker(run_key: str) -> None:
            pass

        assert worker._dispatchio_job.executor.script == __file__

    def test_no_dependencies_by_default(self):
        @job(cadence=DAILY)
        def solo(run_key: str) -> None:
            pass

        assert solo._dispatchio_job.depends_on == []


class TestJobDecoratorDependencies:
    def test_depends_on_decorated_function(self):
        @job(cadence=DAILY)
        def upstream(run_key: str) -> None:
            pass

        @job(cadence=DAILY, depends_on=upstream)
        def downstream(run_key: str) -> None:
            pass

        deps = downstream._dispatchio_job.depends_on
        assert len(deps) == 1
        assert isinstance(deps[0], JobDependency)
        assert deps[0].job_name == "upstream"

    def test_depends_on_list_of_decorated_functions(self):
        @job(cadence=DAILY)
        def step_a(run_key: str) -> None:
            pass

        @job(cadence=DAILY)
        def step_b(run_key: str) -> None:
            pass

        @job(cadence=DAILY, depends_on=[step_a, step_b])
        def final(run_key: str) -> None:
            pass

        dep_names = {d.job_name for d in final._dispatchio_job.depends_on}
        assert dep_names == {"step_a", "step_b"}

    def test_depends_on_job_object(self):
        upstream_job = Job.create(
            "external_job",
            executor=PythonJob(script="/some/script.py", function="run"),
            cadence=DAILY,
        )

        @job(cadence=DAILY, depends_on=upstream_job)
        def consumer(run_key: str) -> None:
            pass

        deps = consumer._dispatchio_job.depends_on
        assert len(deps) == 1
        assert deps[0].job_name == "external_job"


class TestJobDecoratorTransparency:
    def test_function_is_callable(self):
        @job(cadence=DAILY)
        def add(x: int, y: int) -> int:
            return x + y

        assert add(2, 3) == 5

    def test_functools_wraps_preserves_name(self):
        @job(cadence=DAILY)
        def my_worker(run_key: str) -> None:
            """Docstring."""

        assert my_worker.__name__ == "my_worker"
        assert my_worker.__doc__ == "Docstring."

    def test_function_receives_kwargs(self):
        @job(cadence=DAILY)
        def fn(**kw: Any) -> dict:
            return kw

        assert fn(a=1, b=2) == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# run-file CLI command
# ---------------------------------------------------------------------------


def _write_script(tmp_path: Path, content: str) -> Path:
    script = tmp_path / "my_work.py"
    script.write_text(textwrap.dedent(content))
    return script


def _make_fake_orchestrator() -> MagicMock:
    from dispatchio.models import TickResult
    from datetime import datetime, timezone

    orch = MagicMock()
    orch.tick.return_value = TickResult(
        orchestrator_name="default",
        orchestrator_run_id=None,
        orchestrator_run_key="20250115",
        reference_time=datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc),
        jobs=[],
        dry_run=False,
    )
    return orch


class TestRunFileCLIErrors:
    def test_missing_script(self, tmp_path: Path):
        result = runner.invoke(app, ["run-file", str(tmp_path / "missing.py")])
        assert result.exit_code != 0

    def test_no_decorated_functions(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            """\
            def plain_fn(run_key: str) -> None:
                pass
        """,
        )
        result = runner.invoke(app, ["run-file", str(script)])
        assert result.exit_code != 0
        assert "@job" in result.output or "No @job" in result.output


class TestRunFileCLIDiscovery:
    def test_discovers_decorated_jobs(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            """\
            from dispatchio import job, DAILY

            @job(cadence=DAILY)
            def hello_world(run_key: str) -> None:
                pass
        """,
        )

        fake_orch = _make_fake_orchestrator()
        with (
            patch("dispatchio.config.load_config", return_value=MagicMock()),
            patch(
                "dispatchio.config.orchestrator", return_value=fake_orch
            ) as mock_build,
        ):
            result = runner.invoke(app, ["run-file", str(script)])

        assert result.exit_code == 0, result.output
        jobs_passed = mock_build.call_args[0][0]
        assert len(jobs_passed) == 1
        assert jobs_passed[0].name == "hello_world"

    def test_discovers_multiple_jobs_in_declaration_order(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            """\
            from dispatchio import job, DAILY

            @job(cadence=DAILY)
            def first(run_key: str) -> None:
                pass

            @job(cadence=DAILY, depends_on=first)
            def second(run_key: str) -> None:
                pass
        """,
        )

        fake_orch = _make_fake_orchestrator()
        with (
            patch("dispatchio.config.load_config", return_value=MagicMock()),
            patch(
                "dispatchio.config.orchestrator", return_value=fake_orch
            ) as mock_build,
        ):
            result = runner.invoke(app, ["run-file", str(script)])

        assert result.exit_code == 0, result.output
        job_names = [j.name for j in mock_build.call_args[0][0]]
        assert job_names == ["first", "second"]


class TestRunFileRunKey:
    def test_run_key_overrides_cadence_to_literal(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            """\
            from dispatchio import job, DAILY

            @job(cadence=DAILY)
            def my_job(run_key: str) -> None:
                pass
        """,
        )

        fake_orch = _make_fake_orchestrator()
        with (
            patch("dispatchio.config.load_config", return_value=MagicMock()),
            patch(
                "dispatchio.config.orchestrator", return_value=fake_orch
            ) as mock_build,
        ):
            result = runner.invoke(
                app, ["run-file", str(script), "--run-key", "D20250115"]
            )

        assert result.exit_code == 0, result.output
        jobs_passed = mock_build.call_args[0][0]
        assert isinstance(jobs_passed[0].cadence, LiteralCadence)
        assert jobs_passed[0].cadence.value == "D20250115"

    def test_run_key_overrides_dependency_cadences(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            """\
            from dispatchio import job, DAILY

            @job(cadence=DAILY)
            def upstream(run_key: str) -> None:
                pass

            @job(cadence=DAILY, depends_on=upstream)
            def downstream(run_key: str) -> None:
                pass
        """,
        )

        fake_orch = _make_fake_orchestrator()
        with (
            patch("dispatchio.config.load_config", return_value=MagicMock()),
            patch(
                "dispatchio.config.orchestrator", return_value=fake_orch
            ) as mock_build,
        ):
            result = runner.invoke(
                app, ["run-file", str(script), "--run-key", "my-event-123"]
            )

        assert result.exit_code == 0, result.output
        jobs_passed = mock_build.call_args[0][0]
        downstream_job = next(j for j in jobs_passed if j.name == "downstream")
        dep = downstream_job.depends_on[0]
        assert isinstance(dep.cadence, LiteralCadence)
        assert dep.cadence.value == "my-event-123"


class TestRunFileIgnoreDependencies:
    def test_strips_all_deps(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            """\
            from dispatchio import job, DAILY

            @job(cadence=DAILY)
            def upstream(run_key: str) -> None:
                pass

            @job(cadence=DAILY, depends_on=upstream)
            def downstream(run_key: str) -> None:
                pass
        """,
        )

        fake_orch = _make_fake_orchestrator()
        with (
            patch("dispatchio.config.load_config", return_value=MagicMock()),
            patch(
                "dispatchio.config.orchestrator", return_value=fake_orch
            ) as mock_build,
        ):
            result = runner.invoke(
                app, ["run-file", str(script), "--ignore-dependencies"]
            )

        assert result.exit_code == 0, result.output
        for j in mock_build.call_args[0][0]:
            assert j.depends_on == []

    def test_run_key_and_ignore_dependencies_combined(self, tmp_path: Path):
        script = _write_script(
            tmp_path,
            """\
            from dispatchio import job, DAILY

            @job(cadence=DAILY)
            def upstream(run_key: str) -> None:
                pass

            @job(cadence=DAILY, depends_on=upstream)
            def downstream(run_key: str) -> None:
                pass
        """,
        )

        fake_orch = _make_fake_orchestrator()
        with (
            patch("dispatchio.config.load_config", return_value=MagicMock()),
            patch(
                "dispatchio.config.orchestrator", return_value=fake_orch
            ) as mock_build,
        ):
            result = runner.invoke(
                app,
                [
                    "run-file",
                    str(script),
                    "--run-key",
                    "D20250115",
                    "--ignore-dependencies",
                ],
            )

        assert result.exit_code == 0, result.output
        for j in mock_build.call_args[0][0]:
            assert isinstance(j.cadence, LiteralCadence)
            assert j.cadence.value == "D20250115"
            assert j.depends_on == []

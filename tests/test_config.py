"""
Tests for dispatchio.config — settings loading, priority ordering, and
orchestrator_from_config factory.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from dispatchio.config.settings import (
    DispatchioSettings,
    ReceiverSettings,
    StateSettings,
)
from dispatchio.config.loader import (
    _find_config_file,
    load_config,
    orchestrator_from_config,
)
from dispatchio.models import Job, SubprocessJob
from dispatchio.orchestrator import Orchestrator
from dispatchio.state import SQLAlchemyStateStore
from dispatchio.receiver import FilesystemReceiver
from dispatchio_aws.receiver.sqs import SQSReceiver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _toml(content: str, path: Path) -> Path:
    """Write TOML content to a file and return the path."""
    path.write_text(textwrap.dedent(content))
    return path


@pytest.fixture
def simple_job():
    return Job(
        name="j",
        executor=SubprocessJob(command=["echo", "{run_id}"]),
    )


# ---------------------------------------------------------------------------
# Default settings
# ---------------------------------------------------------------------------


class TestDefaultSettings:
    def test_log_level_default(self):
        s = DispatchioSettings()
        assert s.log_level == "INFO"

    def test_state_backend_default(self):
        s = DispatchioSettings()
        assert s.state.backend == "sqlalchemy"
        assert s.state.connection_string == "sqlite:///dispatchio.db"

    def test_receiver_backend_default(self):
        s = DispatchioSettings()
        assert s.receiver.backend == "filesystem"
        assert s.receiver.drop_dir == ".dispatchio/completions"


# ---------------------------------------------------------------------------
# Environment variable overrides
# ---------------------------------------------------------------------------


class TestEnvVarOverrides:
    def test_top_level_env_var(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_LOG_LEVEL", "DEBUG")
        s = DispatchioSettings()
        assert s.log_level == "DEBUG"

    def test_nested_state_backend(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_STATE__BACKEND", "sqlalchemy")
        s = DispatchioSettings()
        assert s.state.backend == "sqlalchemy"

    def test_nested_state_root(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_STATE__CONNECTION_STRING", "sqlite:///custom.db")
        s = DispatchioSettings()
        assert s.state.connection_string == "sqlite:///custom.db"

    def test_nested_receiver_backend(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "none")
        s = DispatchioSettings()
        assert s.receiver.backend == "none"

    def test_nested_receiver_drop_dir(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_RECEIVER__DROP_DIR", "/my/drops")
        s = DispatchioSettings()
        assert s.receiver.drop_dir == "/my/drops"

    def test_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_LOG_LEVEL", "warning")
        s = DispatchioSettings()
        assert (
            s.log_level == "warning"
        )  # stored as-is; logging.getLevelName handles case


# ---------------------------------------------------------------------------
# TOML loading
# ---------------------------------------------------------------------------


class TestTomlLoading:
    def test_bare_toml_file(self, tmp_path):
        f = _toml(
            """
            log_level = "DEBUG"
            [state]
            backend = "sqlalchemy"
            connection_string = "sqlite:///:memory:"
            [receiver]
            backend = "none"
        """,
            tmp_path / "dispatchio.toml",
        )
        s = load_config(f)
        assert s.log_level == "DEBUG"
        assert s.state.backend == "sqlalchemy"
        assert s.receiver.backend == "none"

    def test_dispatchio_section_in_toml(self, tmp_path):
        """[dispatchio] section should be extracted from a larger file."""
        f = _toml(
            """
            [tool.something]
            foo = "bar"

            [dispatchio]
            log_level = "WARNING"

            [dispatchio.state]
            backend = "sqlalchemy"
        """,
            tmp_path / "pyproject.toml",
        )
        s = load_config(f)
        assert s.log_level == "WARNING"
        assert s.state.backend == "sqlalchemy"

    def test_missing_keys_use_defaults(self, tmp_path):
        f = _toml(
            """
            log_level = "DEBUG"
        """,
            tmp_path / "dispatchio.toml",
        )
        s = load_config(f)
        assert s.state.backend == "sqlalchemy"  # default preserved

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/dispatchio.toml")


# ---------------------------------------------------------------------------
# Config file resolution
# ---------------------------------------------------------------------------


class TestConfigFileResolution:
    def test_explicit_path_wins(self, tmp_path):
        f = _toml('log_level = "DEBUG"', tmp_path / "explicit.toml")
        s = load_config(f)
        assert s.log_level == "DEBUG"

    def test_dispatchio_config_env_var(self, tmp_path, monkeypatch):
        f = _toml('log_level = "WARNING"', tmp_path / "from_env.toml")
        monkeypatch.setenv("DISPATCHIO_CONFIG", str(f))
        s = load_config()
        assert s.log_level == "WARNING"

    def test_dispatchio_config_env_var_not_found_raises(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_CONFIG", "/no/such/file.toml")
        with pytest.raises(FileNotFoundError):
            load_config()

    def test_ssm_path_raises_not_implemented(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_CONFIG", "ssm:///myapp/dispatchio")
        with pytest.raises(NotImplementedError, match="dispatchio\\[aws\\]"):
            load_config()

    def test_no_file_returns_defaults(self, tmp_path, monkeypatch):
        """When no config file exists anywhere, defaults are used."""
        monkeypatch.delenv("DISPATCHIO_CONFIG", raising=False)
        # Run with a working directory that has no dispatchio.toml
        monkeypatch.chdir(tmp_path)
        s = load_config()
        assert s.log_level == "INFO"

    def test_find_config_file_explicit(self, tmp_path):
        f = tmp_path / "test.toml"
        f.write_text("")
        assert _find_config_file(f) == f

    def test_find_config_file_missing_explicit_raises(self):
        with pytest.raises(FileNotFoundError):
            _find_config_file("/definitely/does/not/exist.toml")

    def test_find_config_file_none_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DISPATCHIO_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        assert _find_config_file(None) is None


# ---------------------------------------------------------------------------
# Priority: env vars override TOML
# ---------------------------------------------------------------------------


class TestPriority:
    def test_env_overrides_toml(self, tmp_path, monkeypatch):
        f = _toml('log_level = "DEBUG"', tmp_path / "dispatchio.toml")
        monkeypatch.setenv("DISPATCHIO_LOG_LEVEL", "ERROR")
        s = load_config(f)
        assert s.log_level == "ERROR"  # env wins

    def test_env_overrides_nested_toml(self, tmp_path, monkeypatch):
        f = _toml(
            """
            [state]
            backend = "sqlalchemy"
            connection_string = "sqlite:///from_toml.db"
        """,
            tmp_path / "dispatchio.toml",
        )
        monkeypatch.setenv(
            "DISPATCHIO_STATE__CONNECTION_STRING", "sqlite:///from_env.db"
        )
        s = load_config(f)
        assert s.state.connection_string == "sqlite:///from_env.db"  # env wins

    def test_toml_overrides_defaults(self, tmp_path):
        f = _toml('log_level = "WARNING"', tmp_path / "dispatchio.toml")
        s = load_config(f)
        assert s.log_level == "WARNING"  # toml wins over default "INFO"


# ---------------------------------------------------------------------------
# orchestrator_from_config
# ---------------------------------------------------------------------------


class TestOrchestratorFromConfig:
    def test_returns_orchestrator(self, simple_job, tmp_path):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator_from_config([simple_job], config=settings)
        assert isinstance(orch, Orchestrator)

    def test_sqlalchemy_state_backend(self, simple_job, tmp_path):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator_from_config([simple_job], config=settings)
        assert isinstance(orch.state, SQLAlchemyStateStore)

    def test_filesystem_receiver(self, simple_job, tmp_path):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(
                backend="filesystem",
                drop_dir=str(tmp_path / "completions"),
            ),
        )
        orch = orchestrator_from_config([simple_job], config=settings)
        assert isinstance(orch.receiver, FilesystemReceiver)

    def test_no_receiver_when_none(self, simple_job):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator_from_config([simple_job], config=settings)
        assert orch.receiver is None

    def test_accepts_path_to_toml(self, simple_job, tmp_path):
        f = _toml(
            """
            [state]
            backend = "sqlalchemy"
            connection_string = "sqlite:///:memory:"
            [receiver]
            backend = "none"
        """,
            tmp_path / "dispatchio.toml",
        )
        orch = orchestrator_from_config([simple_job], config=f)
        assert isinstance(orch, Orchestrator)

    def test_jobs_are_passed_through(self, simple_job):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator_from_config([simple_job], config=settings)
        assert len(orch.jobs) == 1
        assert orch.jobs[0].name == "j"

    def test_jobs_default_to_empty_list(self):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator_from_config(config=settings)
        assert len(orch.jobs) == 0

    def test_orchestrator_kwargs_forwarded(self, simple_job):
        from dispatchio.alerts.base import LogAlertHandler

        handler = LogAlertHandler()
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator_from_config(
            [simple_job], config=settings, alert_handler=handler
        )
        assert orch.alert_handler is handler

    def test_unknown_state_backend_raises(self, simple_job):
        settings = DispatchioSettings.__new__(DispatchioSettings)
        object.__setattr__(settings, "log_level", "INFO")
        object.__setattr__(
            settings, "state", StateSettings.model_construct(backend="unknown")
        )  # type: ignore
        object.__setattr__(settings, "receiver", ReceiverSettings(backend="none"))
        with pytest.raises(ValueError, match="Unknown state backend"):
            orchestrator_from_config([simple_job], config=settings)

    def test_dynamodb_backend_without_aws_raises(self, simple_job):
        settings = DispatchioSettings(
            state=StateSettings(backend="dynamodb"),
            receiver=ReceiverSettings(backend="none"),
        )
        with pytest.raises(ImportError, match="dispatchio\\[aws\\]"):
            orchestrator_from_config([simple_job], config=settings)

    def test_sqs_backend_builds_receiver(self, simple_job):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(
                backend="sqs",
                queue_url="https://sqs.eu-west-1.amazonaws.com/123456789/dispatchio",
                region="eu-west-1",
            ),
        )
        orch = orchestrator_from_config([simple_job], config=settings)
        assert isinstance(orch.receiver, SQSReceiver)

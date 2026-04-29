"""
Tests for dispatchio.config — settings loading, priority ordering, and
orchestrator factory.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from dispatchio.config.settings import (
    DispatchioSettings,
    ReceiverSettings,
    StateSettings,
)
from dispatchio.config.loader import (
    _CONFIG_INLINE_ENV_VAR,
    _find_config_file,
    load_config,
)
from dispatchio.config.factory import orchestrator
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
        executor=SubprocessJob(command=["echo", "{run_key}"]),
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
        assert s.receiver.backend == "none"

    def test_admission_defaults(self):
        s = DispatchioSettings()
        assert s.admission.max_active_jobs is None
        assert s.admission.max_submit_jobs_per_tick is None
        assert "default" in s.admission.pools


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

    def test_admission_env_override(self, monkeypatch):
        monkeypatch.setenv("DISPATCHIO_ADMISSION__MAX_SUBMIT_JOBS_PER_TICK", "7")
        s = DispatchioSettings()
        assert s.admission.max_submit_jobs_per_tick == 7


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

    def test_admission_section_in_toml(self, tmp_path):
        f = _toml(
            """
            [dispatchio.admission]
            max_active_jobs = 200
            max_submit_jobs_per_tick = 50

            [dispatchio.admission.pools.default]
            max_active_jobs = 100

            [dispatchio.admission.pools.bulk]
            max_submit_jobs_per_tick = 25
        """,
            tmp_path / "dispatchio.toml",
        )
        s = load_config(f)
        assert s.admission.max_active_jobs == 200
        assert s.admission.max_submit_jobs_per_tick == 50
        assert s.admission.pools["default"].max_active_jobs == 100
        assert s.admission.pools["bulk"].max_submit_jobs_per_tick == 25

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
# orchestrator
# ---------------------------------------------------------------------------


class TestOrchestratorFromConfig:
    def test_returns_orchestrator(self, simple_job, tmp_path):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator([simple_job], config=settings)
        assert isinstance(orch, Orchestrator)

    def test_sqlalchemy_state_backend(self, simple_job, tmp_path):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator([simple_job], config=settings)
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
        orch = orchestrator([simple_job], config=settings)
        assert isinstance(orch.receiver, FilesystemReceiver)

    def test_no_receiver_when_none(self, simple_job):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator([simple_job], config=settings)
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
        orch = orchestrator([simple_job], config=f)
        assert isinstance(orch, Orchestrator)

    def test_jobs_are_passed_through(self, simple_job):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator([simple_job], config=settings)
        assert len(orch.jobs) == 1
        assert orch.jobs[0].name == "j"

    def test_jobs_default_to_empty_list(self):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="none"),
        )
        orch = orchestrator(config=settings)
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
        orch = orchestrator([simple_job], config=settings, alert_handler=handler)
        assert orch.alert_handler is handler

    def test_unknown_state_backend_raises(self, simple_job):
        settings = DispatchioSettings.__new__(DispatchioSettings)
        object.__setattr__(settings, "namespace", "default")
        object.__setattr__(settings, "log_level", "INFO")
        object.__setattr__(
            settings, "state", StateSettings.model_construct(backend="unknown")
        )  # type: ignore
        object.__setattr__(settings, "receiver", ReceiverSettings(backend="none"))
        with pytest.raises(ValueError, match="Unknown state backend"):
            orchestrator([simple_job], config=settings)

    def test_dynamodb_backend_without_aws_raises(self, simple_job):
        settings = DispatchioSettings(
            state=StateSettings(backend="dynamodb"),
            receiver=ReceiverSettings(backend="none"),
        )
        with pytest.raises(ImportError, match="dispatchio\\[aws\\]"):
            orchestrator([simple_job], config=settings)

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
        orch = orchestrator([simple_job], config=settings)
        assert isinstance(orch.receiver, SQSReceiver)

    def test_executors_receive_inline_config(self):
        settings = DispatchioSettings(
            state=StateSettings(
                backend="sqlalchemy", connection_string="sqlite:///:memory:"
            ),
            receiver=ReceiverSettings(backend="filesystem", drop_dir="/tmp/drops"),
        )
        orch = orchestrator([], config=settings)
        for name, executor in orch.executors.items():
            assert _CONFIG_INLINE_ENV_VAR in executor._env, (
                f"{name} executor missing inline config"
            )
            data = json.loads(executor._env[_CONFIG_INLINE_ENV_VAR])
            assert data["receiver"]["backend"] == "filesystem"
            assert data["receiver"]["drop_dir"] == "/tmp/drops"


# ---------------------------------------------------------------------------
# Inline config (DISPATCHIO_CONFIG_INLINE)
# ---------------------------------------------------------------------------


class TestInlineConfig:
    def test_inline_loads_settings(self, monkeypatch):
        data = {"receiver": {"backend": "filesystem", "drop_dir": "/tmp/drops"}}
        monkeypatch.setenv(_CONFIG_INLINE_ENV_VAR, json.dumps(data))
        s = load_config()
        assert s.receiver.backend == "filesystem"
        assert s.receiver.drop_dir == "/tmp/drops"

    def test_inline_takes_priority_over_config_file(self, tmp_path, monkeypatch):
        f = _toml('log_level = "DEBUG"', tmp_path / "dispatchio.toml")
        monkeypatch.setenv("DISPATCHIO_CONFIG", str(f))
        monkeypatch.setenv(_CONFIG_INLINE_ENV_VAR, json.dumps({"log_level": "WARNING"}))
        s = load_config()
        assert s.log_level == "WARNING"

    def test_env_vars_override_inline(self, monkeypatch):
        monkeypatch.setenv(_CONFIG_INLINE_ENV_VAR, json.dumps({"log_level": "WARNING"}))
        monkeypatch.setenv("DISPATCHIO_LOG_LEVEL", "ERROR")
        s = load_config()
        assert s.log_level == "ERROR"

    def test_settings_roundtrip_via_inline(self, monkeypatch):
        original = DispatchioSettings(
            receiver=ReceiverSettings(backend="filesystem", drop_dir="/roundtrip"),
        )
        monkeypatch.setenv(_CONFIG_INLINE_ENV_VAR, original.model_dump_json())
        recovered = load_config()
        assert recovered.receiver.backend == "filesystem"
        assert recovered.receiver.drop_dir == "/roundtrip"

    def test_invalid_inline_json_raises(self, monkeypatch):
        monkeypatch.setenv(_CONFIG_INLINE_ENV_VAR, "not-valid-json{{{")
        with pytest.raises(Exception):
            load_config()

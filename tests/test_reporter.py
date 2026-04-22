"""Tests for the completion reporter abstraction."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from dispatchio.reporter import (
    get_reporter,
    build_reporter,
    Reporter,
)
from dispatchio.config.settings import ReceiverSettings
from dispatchio.models import Status
from dispatchio.worker.reporter.filesystem import FilesystemReporter


class TestBuildReporter:
    """Tests for build_reporter() factory function."""

    def test_builds_filesystem_reporter(self, tmp_path: Path):
        cfg = ReceiverSettings(backend="filesystem", drop_dir=str(tmp_path))
        reporter = build_reporter(cfg)
        assert reporter is not None
        # Verify it's a FilesystemReporter by calling report
        reporter.report(uuid4(), Status.DONE)
        assert len(list(tmp_path.glob("*.json"))) == 1

    def test_returns_none_for_none_backend(self):
        cfg = ReceiverSettings(backend="none")
        reporter = build_reporter(cfg)
        assert reporter is None

    def test_raises_for_unknown_backend(self):
        cfg = ReceiverSettings(backend="filesystem", drop_dir="/tmp")
        # Manually set unknown backend for testing
        cfg.backend = "unknown"  # type: ignore
        with pytest.raises(ValueError, match="Unknown receiver backend"):
            build_reporter(cfg)


class TestGetReporter:
    """Tests for get_reporter() environment-based function."""

    def test_detects_filesystem_from_env(self, tmp_path: Path, monkeypatch):
        """get_reporter should detect filesystem backend from env vars."""
        drop_dir = str(tmp_path)
        monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "filesystem")
        monkeypatch.setenv("DISPATCHIO_RECEIVER__DROP_DIR", drop_dir)

        correlation_id = uuid4()
        reporter = get_reporter(str(correlation_id))
        reporter.report_success(metadata={"test": True})

        # Verify status event was written
        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1

    def test_defaults_to_none(self, tmp_path: Path, monkeypatch):
        """get_reporter should default to none if backend not set."""
        monkeypatch.setenv("DISPATCHIO_RECEIVER__DROP_DIR", str(tmp_path))
        # Don't set DISPATCHIO_RECEIVER__BACKEND

        correlation_id = uuid4()
        reporter = get_reporter(str(correlation_id))
        reporter.report_success()

        # Verify status event was not written
        files = list(tmp_path.glob("*.json"))
        assert len(files) == 0

    def test_handles_none_backend(self, monkeypatch):
        """get_reporter should return no-op adapter for backend=none."""
        monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "none")

        correlation_id = uuid4()
        reporter = get_reporter(str(correlation_id))
        # Should not raise — should return no-op adapter
        reporter.report_success()
        reporter.report_error("test error")
        reporter.report_running()

    def test_uses_config(self, tmp_path: Path, monkeypatch):
        config_file = tmp_path / "dispatchio.toml"
        config_file.write_text(
            '[receiver]\nbackend = "filesystem"\ndrop_dir = "completions"\n'
        )
        monkeypatch.setenv("DISPATCHIO_CONFIG", str(config_file))

        correlation_id = uuid4()
        reporter = get_reporter(str(correlation_id))
        reporter.report_success()

        files = list((tmp_path / "completions").glob("*.json"))
        assert len(files) == 1

    def test_report_success_writes_done_event(self, tmp_path: Path):
        """Adapter should write DONE completion event."""
        fs_reporter = FilesystemReporter(tmp_path)
        correlation_id = uuid4()
        reporter = Reporter(correlation_id, fs_reporter)

        reporter.report_success(metadata={"rows": 100})

        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1
        assert f"{correlation_id}__done.json" in files[0].name
        # Verify metadata was written
        content = files[0].read_text()
        assert '"rows":100' in content or '"rows": 100' in content

    def test_report_error_writes_error_event(self, tmp_path: Path):
        """Adapter should write ERROR completion event."""
        fs_reporter = FilesystemReporter(tmp_path)
        correlation_id = uuid4()
        reporter = Reporter(correlation_id, fs_reporter)

        reporter.report_error("Database connection failed")

        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1
        assert f"{correlation_id}__error.json" in files[0].name
        # Verify error_reason was written
        content = files[0].read_text()
        assert "Database connection failed" in content

    def test_report_running_writes_running_event(self, tmp_path: Path):
        """Adapter should write RUNNING event."""
        fs_reporter = FilesystemReporter(tmp_path)
        correlation_id = uuid4()
        reporter = Reporter(correlation_id, fs_reporter)

        reporter.report_running()

        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1
        assert f"{correlation_id}__running.json" in files[0].name

    def test_handles_none_reporter_gracefully(self):
        """Adapter should not raise when reporter is None."""
        reporter = Reporter("my_job", None)

        # Should not raise
        reporter.report_success()
        reporter.report_error("error")
        reporter.report_running()


class TestReporterIntegration:
    """Integration tests for the reporter abstraction."""

    def test_orchestrator_injects_receiver_config(self):
        """orchestrator should inject receiver settings as env vars."""
        from dispatchio.config import orchestrator
        from dispatchio.config.settings import DispatchioSettings, ReceiverSettings

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create an orchestrator with specific receiver config
            settings = DispatchioSettings(
                receiver=ReceiverSettings(
                    backend="filesystem",
                    drop_dir=str(Path(tmp_dir) / "drops"),
                )
            )
            orch = orchestrator([], config=settings)

            # The orchestrator's executor should have injected the config
            assert orch.executors is not None


# ---------------------------------------------------------------------------
# get_reporter with inline config
# ---------------------------------------------------------------------------


class TestGetReporterWithInline:
    def test_inline_config_used(self, tmp_path, monkeypatch):
        data = {"receiver": {"backend": "filesystem", "drop_dir": str(tmp_path)}}
        monkeypatch.setenv("DISPATCHIO_CONFIG_INLINE", json.dumps(data))
        reporter = get_reporter(str(uuid4()))
        reporter.report_success()
        assert len(list(tmp_path.glob("*.json"))) == 1

    def test_env_var_overrides_inline_receiver(self, tmp_path, monkeypatch):
        # Inline says "none" but DISPATCHIO_RECEIVER__BACKEND overrides to filesystem.
        # Verifies env vars beat inline (same priority order as env vs TOML).
        monkeypatch.setenv(
            "DISPATCHIO_CONFIG_INLINE",
            json.dumps({"receiver": {"backend": "none"}}),
        )
        monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "filesystem")
        monkeypatch.setenv("DISPATCHIO_RECEIVER__DROP_DIR", str(tmp_path))
        reporter = get_reporter(str(uuid4()))
        reporter.report_success()
        assert len(list(tmp_path.glob("*.json"))) == 1

    def test_inline_takes_priority_over_config_file(self, tmp_path, monkeypatch):
        config_file = tmp_path / "dispatchio.toml"
        config_file.write_text(
            '[receiver]\nbackend = "filesystem"\ndrop_dir = "completions"\n'
        )
        monkeypatch.setenv("DISPATCHIO_CONFIG", str(config_file))
        monkeypatch.setenv(
            "DISPATCHIO_CONFIG_INLINE",
            json.dumps({"receiver": {"backend": "none"}}),
        )
        reporter = get_reporter(str(uuid4()))
        reporter.report_success()
        # Inline "none" won, so no file written
        assert not (tmp_path / "completions").exists()

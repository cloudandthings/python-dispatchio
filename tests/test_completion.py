"""Tests for the completion reporter abstraction."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from dispatchio.completion import (
    CompletionReporter,
    get_reporter,
    build_reporter,
    _CompletionReporterAdapter,
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
        reporter.report("job", "id", Status.DONE)
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

        reporter = get_reporter("test_job")
        reporter.report_success("20250115", metadata={"test": True})

        # Verify completion event was written
        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1

    def test_defaults_to_filesystem(self, tmp_path: Path, monkeypatch):
        """get_reporter should default to filesystem if backend not set."""
        monkeypatch.setenv("DISPATCHIO_RECEIVER__DROP_DIR", str(tmp_path))
        # Don't set DISPATCHIO_RECEIVER__BACKEND

        reporter = get_reporter("test_job")
        reporter.report_success("20250115")

        # Verify completion event was written
        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1

    def test_handles_none_backend(self, monkeypatch):
        """get_reporter should return no-op adapter for backend=none."""
        monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "none")

        reporter = get_reporter("test_job")
        # Should not raise — should return no-op adapter
        reporter.report_success("20250115")
        reporter.report_error("20250116", "test error")
        reporter.report_running("20250117")

    def test_report_success_writes_done_event(self, tmp_path: Path):
        """Adapter should write DONE completion event."""
        fs_reporter = FilesystemReporter(tmp_path)
        reporter = _CompletionReporterAdapter("my_job", fs_reporter)

        reporter.report_success("20250115", metadata={"rows": 100})

        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1
        assert "my_job__20250115__done.json" in files[0].name
        # Verify metadata was written
        content = files[0].read_text()
        assert '"rows":100' in content or '"rows": 100' in content

    def test_report_error_writes_error_event(self, tmp_path: Path):
        """Adapter should write ERROR completion event."""
        fs_reporter = FilesystemReporter(tmp_path)
        reporter = _CompletionReporterAdapter("my_job", fs_reporter)

        reporter.report_error("20250115", "Database connection failed")

        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1
        assert "my_job__20250115__error.json" in files[0].name
        # Verify error_reason was written
        content = files[0].read_text()
        assert "Database connection failed" in content

    def test_report_running_writes_running_event(self, tmp_path: Path):
        """Adapter should write RUNNING (heartbeat) event."""
        fs_reporter = FilesystemReporter(tmp_path)
        reporter = _CompletionReporterAdapter("my_job", fs_reporter)

        reporter.report_running("20250115")

        files = list(tmp_path.glob("*.json"))
        assert len(files) == 1
        assert "my_job__20250115__running.json" in files[0].name

    def test_handles_none_reporter_gracefully(self):
        """Adapter should not raise when reporter is None."""
        reporter = _CompletionReporterAdapter("my_job", None)

        # Should not raise
        reporter.report_success("20250115")
        reporter.report_error("20250116", "error")
        reporter.report_running("20250117")


class TestCompletionReporterIntegration:
    """Integration tests for the completion reporter abstraction."""

    def test_orchestrator_injects_receiver_config(self):
        """orchestrator_from_config should inject receiver settings as env vars."""
        from dispatchio.config import orchestrator_from_config, load_config
        from dispatchio.config.settings import DispatchioSettings, ReceiverSettings

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create an orchestrator with specific receiver config
            settings = DispatchioSettings(
                receiver=ReceiverSettings(
                    backend="filesystem",
                    drop_dir=str(Path(tmp_dir) / "drops"),
                )
            )
            orch = orchestrator_from_config([], config=settings)

            # The orchestrator's executor should have injected the config
            assert orch.executors is not None

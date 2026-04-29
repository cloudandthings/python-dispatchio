from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from dispatchio.config.settings import (
    DispatchioSettings,
    ReceiverSettings,
    StateSettings,
)
from dispatchio.models import Job, SubprocessJob
from dispatchio_aws.config import (
    _ssm_parameter_name,
    aws_orchestrator,
    load_config,
)
from dispatchio_aws.executor.athena import AthenaExecutor
from dispatchio_aws.executor.lambda_ import LambdaExecutor
from dispatchio_aws.executor.stepfunctions import StepFunctionsExecutor

_TOML_CONFIG = """
[state]
backend = "sqlalchemy"
connection_string = "sqlite:///:memory:"

[receiver]
backend = "none"
"""


def test_aws_orchestrator_registers_aws_executors() -> None:
    settings = DispatchioSettings(
        state=StateSettings(
            backend="sqlalchemy", connection_string="sqlite:///:memory:"
        ),
        receiver=ReceiverSettings(backend="none"),
    )
    jobs = [Job(name="j", executor=SubprocessJob(command=["echo", "{run_key}"]))]

    orch = aws_orchestrator(jobs=jobs, config=settings)

    assert "lambda" in orch.executors
    assert "stepfunctions" in orch.executors
    assert "athena" in orch.executors
    assert isinstance(orch.executors["lambda"], LambdaExecutor)
    assert isinstance(orch.executors["stepfunctions"], StepFunctionsExecutor)
    assert isinstance(orch.executors["athena"], AthenaExecutor)


class TestSsmParameterName:
    def test_triple_slash_form(self) -> None:
        assert _ssm_parameter_name("ssm:///myapp/dispatchio") == "/myapp/dispatchio"

    def test_double_slash_form(self) -> None:
        assert _ssm_parameter_name("ssm://myapp/dispatchio") == "myapp/dispatchio"

    def test_simple_name(self) -> None:
        assert _ssm_parameter_name("ssm:///dispatchio") == "/dispatchio"

    def test_wrong_scheme_raises(self) -> None:
        with pytest.raises(ValueError, match="ssm://"):
            _ssm_parameter_name("s3://bucket/key")

    def test_empty_name_raises(self) -> None:
        with pytest.raises(ValueError, match="No parameter name"):
            _ssm_parameter_name("ssm://")


class TestSsmLoadConfig:
    @mock_aws
    def test_loads_toml_from_ssm(self) -> None:
        boto3.client("ssm").put_parameter(
            Name="/myapp/dispatchio",
            Value=_TOML_CONFIG,
            Type="String",
        )
        settings = load_config("ssm:///myapp/dispatchio")
        assert settings.state.backend == "sqlalchemy"
        assert settings.receiver.backend == "none"

    @mock_aws
    def test_loads_toml_with_dispatchio_section(self) -> None:
        toml_with_section = "[dispatchio]\n" + _TOML_CONFIG
        boto3.client("ssm").put_parameter(
            Name="/myapp/cfg",
            Value=toml_with_section,
            Type="String",
        )
        settings = load_config("ssm:///myapp/cfg")
        assert settings.state.backend == "sqlalchemy"

    @mock_aws
    def test_env_var_ssm_path(self, monkeypatch) -> None:
        boto3.client("ssm").put_parameter(
            Name="/myapp/dispatchio",
            Value=_TOML_CONFIG,
            Type="String",
        )
        monkeypatch.setenv("DISPATCHIO_CONFIG", "ssm:///myapp/dispatchio")
        settings = load_config()
        assert settings.state.backend == "sqlalchemy"

    @mock_aws
    def test_invalid_toml_raises(self) -> None:
        boto3.client("ssm").put_parameter(
            Name="/bad/config",
            Value="this is not toml ::::",
            Type="String",
        )
        with pytest.raises(ValueError, match="not valid TOML"):
            load_config("ssm:///bad/config")

    @mock_aws
    def test_env_vars_still_override_ssm_values(self, monkeypatch) -> None:
        boto3.client("ssm").put_parameter(
            Name="/myapp/dispatchio",
            Value=_TOML_CONFIG,
            Type="String",
        )
        monkeypatch.setenv("DISPATCHIO_LOG_LEVEL", "DEBUG")
        settings = load_config("ssm:///myapp/dispatchio")
        assert settings.log_level == "DEBUG"

    @mock_aws
    def test_aws_orchestrator_with_ssm_config(self) -> None:
        boto3.client("ssm").put_parameter(
            Name="/myapp/dispatchio",
            Value=_TOML_CONFIG,
            Type="String",
        )
        jobs = [Job(name="j", executor=SubprocessJob(command=["echo", "hi"]))]
        orch = aws_orchestrator(jobs=jobs, config="ssm:///myapp/dispatchio")
        assert "athena" in orch.executors

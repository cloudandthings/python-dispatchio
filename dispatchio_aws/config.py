from __future__ import annotations

import logging
import os
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from beartype import beartype

from dispatchio.config.factory import orchestrator
from dispatchio.config.loader import load_config as _core_load_config
from dispatchio.config.settings import DispatchioSettings
from dispatchio.models import Job
from dispatchio.orchestrator import Orchestrator
from dispatchio_aws.executor.athena import AthenaExecutor
from dispatchio_aws.executor.lambda_ import LambdaExecutor
from dispatchio_aws.executor.stepfunctions import StepFunctionsExecutor

logger = logging.getLogger(__name__)

_CONFIG_ENV_VAR = "DISPATCHIO_CONFIG"


def _ssm_parameter_name(ssm_uri: str) -> str:
    """Extract the SSM parameter name from an ssm:// URI.

    ssm:///myapp/dispatchio  →  /myapp/dispatchio
    ssm://myapp/dispatchio   →  myapp/dispatchio  (netloc + path)
    """
    parsed = urlparse(ssm_uri)
    if parsed.scheme != "ssm":
        raise ValueError(f"Expected ssm:// URI, got: {ssm_uri!r}")
    # Standard form is ssm:///param/path (empty netloc, path starts with /)
    # Also accept ssm://param/path (netloc is the first path segment)
    if parsed.netloc:
        name = parsed.netloc + parsed.path
    else:
        name = parsed.path
    if not name:
        raise ValueError(f"No parameter name found in SSM URI: {ssm_uri!r}")
    return name


def _fetch_ssm_config(ssm_uri: str, region: str | None = None) -> dict[str, Any]:
    """Fetch a dispatchio TOML config from SSM Parameter Store.

    The parameter value must be a TOML string — either a bare dispatchio.toml
    or a file containing a [dispatchio] section.
    """
    name = _ssm_parameter_name(ssm_uri)
    client = boto3.client("ssm", region_name=region)
    logger.debug("Fetching dispatchio config from SSM: %s", name)
    response = client.get_parameter(Name=name, WithDecryption=True)
    toml_content = response["Parameter"]["Value"]

    try:
        raw = tomllib.loads(toml_content)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"SSM parameter {name!r} is not valid TOML: {exc}") from exc

    # Support both a bare config and a file with a [dispatchio] section
    return raw.get("dispatchio", raw)


def load_config(path: str | Path | None = None) -> DispatchioSettings:
    """Like dispatchio.config.load_config, but additionally supports ssm:// URIs.

    When DISPATCHIO_CONFIG (or the explicit path argument) starts with ssm://,
    the config TOML is fetched from AWS SSM Parameter Store and loaded directly.
    All other resolution and env-var override behaviour is identical to the
    core loader.

    Examples:
        # Operator local workflow: config lives in SSM, state store in the config
        DISPATCHIO_CONFIG=ssm:///myapp/dispatchio dispatchio status

        # Programmatic use:
        from dispatchio_aws.config import load_config
        settings = load_config("ssm:///myapp/dispatchio")
    """
    ssm_ref = str(path) if path is not None else os.environ.get(_CONFIG_ENV_VAR, "")
    if ssm_ref.startswith("ssm://"):
        data = _fetch_ssm_config(ssm_ref)
        # Pass the resolved env var through as the path key so get_config
        # caching (keyed by path) still works correctly for callers.
        return _core_load_config(_preloaded_data=data)
    return _core_load_config(path)


@beartype
def aws_orchestrator(
    jobs: list[Job] | None = None,
    config: str | Path | DispatchioSettings | None = None,
    **orchestrator_kwargs,
) -> Orchestrator:
    """Build an Orchestrator with both core and AWS executors enabled.

    Accepts ssm:// URIs in the config argument (and respects DISPATCHIO_CONFIG
    if config is None), in addition to local file paths and DispatchioSettings.
    """
    if isinstance(config, DispatchioSettings):
        settings = config
    else:
        settings = load_config(config)

    o = orchestrator(
        jobs=jobs,
        config=settings,
        **orchestrator_kwargs,
    )
    o.executors.update(
        {
            "lambda": LambdaExecutor(),
            "stepfunctions": StepFunctionsExecutor(),
            "athena": AthenaExecutor(),
        }
    )
    return o

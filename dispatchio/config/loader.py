"""
Config loader and orchestrator factory.

load_config()            — resolve a config file, merge with env vars, return DispatchioSettings.
orchestrator() — build a fully-wired Orchestrator from settings.
"""

from __future__ import annotations

import json
import logging
import os
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from dispatchio.config.settings import (
    DataStoreSettings,
    DispatchioSettings,
    ReceiverSettings,
    StateSettings,
)
from dispatchio.config.sources.toml_ import TomlSource
from dispatchio.executor import SubprocessExecutor, PythonJobExecutor
from dispatchio.models import Job
from dispatchio.orchestrator import Orchestrator
from dispatchio.receiver import FilesystemReceiver
from dispatchio.state import SQLAlchemyStateStore
from dispatchio.tick_log import FilesystemTickLogStore

logger = logging.getLogger(__name__)

_CONFIG_ENV_VAR = "DISPATCHIO_CONFIG"
_CONFIG_INLINE_ENV_VAR = "DISPATCHIO_CONFIG_INLINE"
_SEARCH_PATHS = ["dispatchio.toml", "~/.dispatchio.toml"]


# ---------------------------------------------------------------------------
# Config file resolution
# ---------------------------------------------------------------------------


def _find_config_file(path: str | Path | None) -> Path | None:
    """
    Resolve a config file path using the lookup chain:
    1. Explicit `path` argument
    2. DISPATCHIO_CONFIG environment variable
    3. ./dispatchio.toml
    4. ~/.dispatchio.toml

    Returns None if no file is found and no explicit path was given.
    Raises FileNotFoundError if an explicit path was given but doesn't exist.
    """
    if path is not None:
        p = _path_from_config_ref(path, source="explicit path")
        if not p.exists():
            raise FileNotFoundError(f"Dispatchio config file not found: {p}")
        return p

    env_val = os.environ.get(_CONFIG_ENV_VAR)
    if env_val:
        p = _path_from_config_ref(env_val, source=_CONFIG_ENV_VAR)
        if not p.exists():
            raise FileNotFoundError(
                f"Config file from {_CONFIG_ENV_VAR}={env_val!r} not found: {p}"
            )
        return p

    for candidate in _SEARCH_PATHS:
        p = Path(candidate).expanduser()
        if p.exists():
            return p

    return None


def _path_from_config_ref(value: str | Path, *, source: str) -> Path:
    """
    Resolve a config reference into a local file path.

    Supports:
      - plain paths
      - file:// URIs

    ssm:// URIs are intentionally not implemented in this package yet.
    """
    if isinstance(value, Path):
        return value.expanduser()

    if value.startswith("ssm://"):
        raise NotImplementedError(
            "SSM config sources require dispatchio[aws]. "
            f"Set {_CONFIG_ENV_VAR} to a local file path, or install "
            "dispatchio[aws] for SSM support."
        )

    if value.startswith("file://"):
        parsed = urlparse(value)
        if parsed.scheme != "file":
            raise ValueError(f"Unsupported config URI scheme in {source}: {value!r}")
        if parsed.netloc not in ("", "localhost"):
            raise ValueError(
                "Only local file:// URIs are supported "
                f"for {source}; got netloc={parsed.netloc!r}"
            )
        decoded_path = unquote(parsed.path)
        if not decoded_path:
            raise ValueError(f"Invalid file URI for {source}: {value!r}")
        return Path(decoded_path).expanduser()

    if "://" in value:
        raise ValueError(f"Unsupported config URI in {source}: {value!r}")

    return Path(value).expanduser()


def _read_toml(path: Path) -> dict[str, Any]:
    """
    Read a TOML file and return the [dispatchio] section if present,
    otherwise return the whole file as the config dict.

    This lets dispatchio.toml be a dedicated file (no section header needed),
    or it can live as a [dispatchio] section inside an existing project file
    such as pyproject.toml.

    Relative path values (state.root, receiver.drop_dir) are resolved
    relative to the directory containing the config file, not the process
    working directory. This ensures config files are portable regardless of
    where dispatchio is invoked from.
    """
    with open(path, "rb") as f:
        data = tomllib.load(f)
    dispatchio_data = data.get("dispatchio", data)
    return _resolve_relative_paths(dispatchio_data, base_dir=path.parent)


def _resolve_relative_paths(data: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Resolve relative path strings in config relative to base_dir."""
    import copy

    data = copy.deepcopy(data)
    _PATH_FIELDS = {
        "state": ["root"],
        "receiver": ["drop_dir"],
        "data_store": ["base_dir"],
    }
    for section, keys in _PATH_FIELDS.items():
        if section not in data:
            continue
        for key in keys:
            val = data[section].get(key)
            if val and not Path(val).is_absolute():
                data[section][key] = str(base_dir / val)
    return data


# TODO refactor into DispatchioSettings method
# Build a one-shot subclass that injects the TOML data as a settings source.
# Priority stack: env vars (auto) > TOML > defaults.
class _Settings(DispatchioSettings):
    _data = {}  # type: ignore[assignment]

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            init_settings,
            env_settings,
            TomlSource(settings_cls, cls._data),
        )


# ---------------------------------------------------------------------------
# Public: load_config
# ---------------------------------------------------------------------------


def load_config(path: str | Path | None = None) -> DispatchioSettings:
    """
    Load DispatchioSettings by merging a config file, environment variables,
    and built-in defaults.

    Priority (highest first):
      1. Environment variables  (DISPATCHIO_ prefix, __ for nested fields)
      2. Config file values     (TOML)
      3. Built-in defaults

    Config file lookup order (first match wins):
      1. Explicit `path` argument
      2. DISPATCHIO_CONFIG_INLINE environment variable (raw TOML content)
      3. DISPATCHIO_CONFIG environment variable (local path or ssm:// with dispatchio[aws])
      4. ./dispatchio.toml
      5. ~/.dispatchio.toml

    If no config file is found, settings come from env vars and defaults only
    — this is perfectly valid for container-based deployments that use only
    environment variables.

    Examples:
        settings = load_config()                     # auto-discover
        settings = load_config("config/prod.toml")   # explicit file
        settings = load_config(Path("/etc/dispatchio/dispatchio.toml"))
    """
    inline = os.environ.get(_CONFIG_INLINE_ENV_VAR)
    if inline:
        data = json.loads(inline)
    else:
        config_path = _find_config_file(path)
        toml_data: dict[str, Any] = {}

        if config_path is not None:
            toml_data = _read_toml(config_path)
            logger.debug("Loaded Dispatchio config from %s", config_path)
        else:
            logger.debug(
                "No Dispatchio config file found — using env vars and defaults"
            )

        data = toml_data

    _Settings._data = data
    return _Settings()


# ---------------------------------------------------------------------------
# Public: orchestrator
# ---------------------------------------------------------------------------


def orchestrator(
    jobs: list[Job] | None = None,
    config: str | Path | DispatchioSettings | None = None,
    **orchestrator_kwargs,
) -> Orchestrator:
    """
    Build a fully-wired Orchestrator from a config file or DispatchioSettings object.

    This is the recommended entry point for non-trivial deployments. It reads
    infrastructure settings (state backend, receiver, log level) from config so
    job definitions stay decoupled from environment-specific values.

    Args:
        jobs:   Optional list of Jobs to evaluate on each tick.
            If omitted, an empty orchestrator is created and jobs can be
            added later via Orchestrator.add_job(s).
        config: One of:
            - None            auto-discover config file (see load_config)
            - str / Path      explicit path to a TOML config file
            - DispatchioSettings pre-built settings object (skips file loading)
        **orchestrator_kwargs:
            Forwarded directly to Orchestrator (e.g. alert_handler=...).

    Example — minimal jobs.py:

        from dispatchio import Job, SubprocessConfig
        from dispatchio.config import orchestrator

        JOBS = [Job(name="etl", executor=SubprocessConfig(...))]
        orchestrator = orchestrator(JOBS)   # reads dispatchio.toml

        # Orchestrator-first flow (dynamic registration):
        # orchestrator = orchestrator()
        # orchestrator.add_jobs(JOBS)
    """
    if isinstance(config, DispatchioSettings):
        settings = config
    else:
        settings = load_config(config)

    _configure_logging(settings.log_level)

    # Allow callers (e.g. orchestrator_from_graph) to override the namespace from
    # settings by passing namespace= in orchestrator_kwargs.
    namespace = orchestrator_kwargs.pop(
        "namespace", settings.namespace
    )

    data_store = _build_data_store(getattr(settings, "data_store", None), namespace)

    executor_env: dict[str, str] = {
        "DISPATCHIO_CONFIG_INLINE": json.dumps(settings.model_dump(mode="json")),
    }

    return Orchestrator(
        jobs=jobs or [],
        state=_build_state(settings.state, namespace=namespace),
        executors={
            "subprocess": SubprocessExecutor(env=executor_env),
            "python": PythonJobExecutor(env=executor_env),
        },
        receiver=_build_receiver(settings.receiver),
        admission_policy=settings.admission,
        default_cadence=settings.default_cadence,
        namespace=namespace,
        tick_log=_build_tick_log(settings.state),
        data_store=data_store,
        **orchestrator_kwargs,
    )


# ---------------------------------------------------------------------------
# Internal: backend construction
# ---------------------------------------------------------------------------


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=False,  # don't override if caller already configured logging
    )


def _build_tick_log(cfg: StateSettings) -> FilesystemTickLogStore:
    """Build a FilesystemTickLogStore from the configured tick_log_path."""
    return FilesystemTickLogStore(Path(cfg.tick_log_path))


def _build_state(cfg: StateSettings, namespace: str | None = "default"):
    if cfg.backend == "sqlalchemy":
        return SQLAlchemyStateStore(
            namespace=namespace,
            connection_string=cfg.connection_string,
            echo=cfg.db_echo,
            pool_size=cfg.db_pool_size,
        )

    if cfg.backend == "dynamodb":
        try:
            from dispatchio_aws.state.dynamodb import DynamoDBStateStore  # type: ignore[import]

            return DynamoDBStateStore(table_name=cfg.table_name, region=cfg.region)
        except ImportError:
            raise ImportError(
                "State backend 'dynamodb' requires dispatchio[aws]. "
                "Install with: pip install dispatchio[aws]"
            )

    raise ValueError(f"Unknown state backend: {cfg.backend!r}")


def _build_data_store(
    cfg: DataStoreSettings | None, default_namespace: str = "default"
):
    if cfg is None or cfg.backend == "none":
        return None
    if cfg.backend == "filesystem":
        from dispatchio.datastore import FilesystemDataStore

        ns = cfg.namespace if cfg.namespace != "default" else default_namespace
        return FilesystemDataStore(cfg.base_dir, namespace=ns)
    raise ValueError(f"Unknown data_store backend: {cfg.backend!r}")


def _build_receiver(cfg: ReceiverSettings):
    if cfg.backend == "none":
        return None

    if cfg.backend == "filesystem":
        return FilesystemReceiver(cfg.drop_dir)

    if cfg.backend == "sqs":
        try:
            from dispatchio_aws.receiver.sqs import SQSReceiver  # type: ignore[import]

            return SQSReceiver(queue_url=cfg.queue_url, region=cfg.region)
        except ImportError:
            raise ImportError(
                "Receiver backend 'sqs' requires dispatchio[aws]. "
                "Install with: pip install dispatchio[aws]"
            )

    raise ValueError(f"Unknown receiver backend: {cfg.backend!r}")

"""
Orchestrator factory.

orchestrator() — build a fully-wired Orchestrator from a config file or
                 DispatchioSettings object.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from dispatchio.state.base import StateStore
from dispatchio.config.loader import load_config
from dispatchio.config.settings import (
    DataStoreSettings,
    DispatchioSettings,
    ReceiverSettings,
    StateSettings,
)
from dispatchio.executor import PythonJobExecutor, SubprocessExecutor
from dispatchio.models import Job
from dispatchio.orchestrator import Orchestrator
from dispatchio.tick_log import FilesystemTickLogStore

logger = logging.getLogger(__name__)


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
    namespace = orchestrator_kwargs.pop("namespace", settings.namespace)

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
        settings=settings,
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
    return FilesystemTickLogStore(Path(cfg.tick_log_path))


def _build_state(cfg: StateSettings, namespace: str | None = "default") -> StateStore:
    if cfg.backend == "sqlalchemy":
        from dispatchio.state import SQLAlchemyStateStore

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
        from dispatchio.receiver import FilesystemReceiver

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

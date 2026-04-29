from dispatchio.config.settings import (
    CliSettings,
    DispatchioSettings,
    ReceiverSettings,
    StateSettings,
    TableSettings,
)
from dispatchio.config.loader import get_config, load_config
from dispatchio.models import AdmissionPolicy, PoolPolicy

__all__ = [
    "CliSettings",
    "DispatchioSettings",
    "StateSettings",
    "ReceiverSettings",
    "TableSettings",
    "AdmissionPolicy",
    "PoolPolicy",
    "get_config",
    "load_config",
    "orchestrator",
    # name field is on DispatchioSettings — no separate export needed
]


def __getattr__(name: str):
    if name == "orchestrator":
        from dispatchio.config.factory import orchestrator

        return orchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

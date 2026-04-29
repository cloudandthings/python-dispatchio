"""
Dispatchio configuration models.

DispatchioSettings is a pydantic-settings BaseSettings class. It reads values from
(in priority order, highest first):

  1. Explicit keyword arguments passed to DispatchioSettings(...)
  2. Environment variables  (DISPATCHIO_ prefix, __ for nested fields)
  3. Config file           (TOML — injected via load_config())
  4. Built-in defaults

Environment variable reference
───────────────────────────────
Prefix all variables with DISPATCHIO_ and use double-underscore (__) to address
nested fields:

  Top-level:
    DISPATCHIO_LOG_LEVEL=DEBUG

  State backend:
    DISPATCHIO_STATE__BACKEND=dynamodb
    DISPATCHIO_STATE__TABLE_NAME=my-table
    DISPATCHIO_STATE__REGION=eu-west-1
    DISPATCHIO_STATE__ROOT=/var/dispatchio/state      # filesystem only

  Receiver backend:
    DISPATCHIO_RECEIVER__BACKEND=sqs
    DISPATCHIO_RECEIVER__QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/123/dispatchio
    DISPATCHIO_RECEIVER__REGION=eu-west-1
    DISPATCHIO_RECEIVER__DROP_DIR=/var/dispatchio/completions  # filesystem only
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from dispatchio.models import AdmissionPolicy


# ---------------------------------------------------------------------------
# Sub-models (plain BaseModel — not BaseSettings themselves)
# ---------------------------------------------------------------------------


class StateSettings(BaseModel):
    """
    State store backend configuration.

    backend="sqlalchemy"  — any SQLAlchemy-compatible DB (default: SQLite).
                           connection_string defaults to "sqlite:///dispatchio.db".
    backend="dynamodb"    — AWS DynamoDB; requires dispatchio[aws].

    Environment variable reference:
      DISPATCHIO_STATE__BACKEND=sqlalchemy
      DISPATCHIO_STATE__CONNECTION_STRING=postgresql+psycopg://user:pass@host/db
      DISPATCHIO_STATE__DB_ECHO=false
      DISPATCHIO_STATE__DB_POOL_SIZE=5
    """

    backend: Literal["sqlalchemy", "dynamodb"] = "sqlalchemy"

    # sqlalchemy
    connection_string: str = "sqlite:///dispatchio.db"
    db_echo: bool = False
    db_pool_size: int = 5

    # tick log (path to the JSONL tick history file)
    tick_log_path: str = ".dispatchio/tick_log.jsonl"

    # dynamodb (dispatchio[aws])
    table_name: str = "dispatchio-state"
    region: str | None = None


class ReceiverSettings(BaseModel):
    """
    Status event receiver configuration.

    backend="filesystem"  — file-drop directory polled each tick; matches FilesystemReporter.
    backend="sqs"         — AWS SQS queue; requires dispatchio[aws].
    backend="none"        — no receiver; jobs must write directly to the state store.
    """

    backend: Literal["filesystem", "sqs", "none"] = "none"

    # filesystem
    drop_dir: str | None = None

    # sqs (dispatchio[aws])
    queue_url: str | None = None
    region: str | None = None


# ---------------------------------------------------------------------------
# Top-level settings
# ---------------------------------------------------------------------------


class DateSettings(BaseModel):
    """
    Date context settings for parametrized runs.

    week_start_day:     First day of the week (0=Monday … 6=Sunday). Default: 0.
    quarter_start_month: First month of Q1 (1=January). Default: 1.
    """

    week_start_day: int = 0
    quarter_start_month: int = 1


class DataStoreSettings(BaseModel):
    """
    Inter-job DataStore configuration.

    backend="filesystem"  — JSON files under base_dir; suitable for local dev
                           and simple single-node deployments.
    backend="none"        — DataStore disabled (default).

    Environment variable reference:
      DISPATCHIO_DATA_STORE__BACKEND=filesystem
      DISPATCHIO_DATA_STORE__BASE_DIR=.dispatchio/data
      DISPATCHIO_DATA_STORE__NAMESPACE=my-pipeline
    """

    backend: Literal["filesystem", "none"] = "none"
    base_dir: str = ".dispatchio/data"
    namespace: str = "default"


class TableSettings(BaseModel):
    """
    Rich Table look-and-feel options applied to all CLI tables.

    box:          Box style name from rich.box (e.g. "ROUNDED", "SIMPLE",
                  "MINIMAL", "HORIZONTALS", "ASCII", "MARKDOWN", "HEAVY").
                  Use "NONE" to remove all borders. Omit to keep Rich's default
                  (HEAVY_HEAD).
    header_style: Rich markup style for column headers. Default: "bold".
    show_lines:   Draw a separator line between every row. Default: false.
    show_edge:    Draw the outer table border. Default: true.
    row_styles:   List of styles cycled across rows for alternating colours,
                  e.g. ["", "dim"] for zebra-striping.
    expand:       Expand the table to fill the terminal width. Default: false.
    border_style: Rich markup style applied to all border characters,
                  e.g. "dim" or "bright_black".

    Example (dispatchio.toml):

        [dispatchio.cli.table]
        box          = "ROUNDED"
        header_style = "bold cyan"
        show_lines   = true
        row_styles   = ["", "dim"]
    """

    box: str | None = None
    header_style: str = "bold"
    show_lines: bool = False
    show_edge: bool = True
    row_styles: list[str] = Field(default_factory=list)
    expand: bool = False
    border_style: str | None = None


class CliSettings(BaseModel):
    """
    CLI look-and-feel configuration.

    Keys in status_colors and action_icons are merged with built-in defaults;
    only specified keys are overridden.

    Example (dispatchio.toml):

        [dispatchio.cli.status_colors]
        done = "bright_green"
        error = "bold red"

        [dispatchio.cli.action_icons]
        submitted = "✅"
        skipped_condition = ""
    """

    status_colors: dict[str, str] = Field(default_factory=dict)
    action_icons: dict[str, str] = Field(default_factory=dict)
    table: TableSettings = Field(default_factory=TableSettings)


class DispatchioSettings(BaseSettings):
    """
    Top-level Dispatchio runtime configuration.

    Typical usage — auto-discover config file then apply env overrides:

        from dispatchio.config import load_config
        settings = load_config()               # reads dispatchio.toml if present
        settings = load_config("prod.toml")    # explicit file

    Or build programmatically (env vars still apply):

        settings = DispatchioSettings(
            log_level="DEBUG",
            state=StateSettings(backend="sqlalchemy", connection_string="sqlite:///:memory:"),
        )

    See module docstring for the full env var reference.
    """

    model_config = SettingsConfigDict(
        env_prefix="DISPATCHIO_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    namespace: str = "default"
    log_level: str = "INFO"
    state: StateSettings = Field(default_factory=StateSettings)
    receiver: ReceiverSettings = Field(default_factory=ReceiverSettings)
    admission: AdmissionPolicy = Field(default_factory=AdmissionPolicy)
    data_store: DataStoreSettings = Field(default_factory=DataStoreSettings)
    dates: DateSettings = Field(default_factory=DateSettings)
    cli: CliSettings = Field(default_factory=CliSettings)
    default_cadence: Any = "daily"
    # Accepts a frequency string ("daily", "monthly", etc.) or a full
    # Cadence dict.  Coerced to a DateCadence by _coerce_cadence below.

    @model_validator(mode="after")
    def _coerce_cadence(self) -> DispatchioSettings:
        from dispatchio.cadence import DateCadence, Frequency

        v = self.default_cadence
        if isinstance(v, str):
            self.default_cadence = DateCadence(frequency=Frequency(v))
        elif isinstance(v, dict):
            self.default_cadence = DateCadence.model_validate(v)
        return self

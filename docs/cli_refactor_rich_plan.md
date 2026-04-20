# CLI Refactor Plan (Submodules + Rich)

## Goal

Refactor the CLI into clear submodules so it scales with new commands, and move output rendering to Rich immediately (alpha-friendly breaking changes are acceptable).

## Design decisions confirmed

1. Separate all command groups and root commands into their own modules.
2. Move directly to Rich for console output (no temporary Typer-style output layer).
3. Keep context command module name as `ctx.py`.
4. `main.py` is deleted — `app.py` is the canonical module and entry point.

## Target structure

```text
dispatchio/cli/
├── __init__.py
├── app.py                     # app construction, entry point, sub-app registration
├── errors.py                  # CliUserError exception type
├── root.py                    # top-level commands: tick/run/run-script/status/ticks/cancel
├── options.py                 # shared Annotated option/argument aliases
├── loaders.py                 # orchestrator/store/tick-log loading helpers
├── output.py                  # Rich console singleton, tables, shared output functions
├── record.py                  # record sub-app + record commands
├── retry.py                   # retry sub-app + retry commands
├── ctx.py                     # context sub-app + context commands
└── graph.py                   # graph sub-app + graph commands
```

## Module responsibilities

### app.py

- The canonical entry point module. `pyproject.toml` points here: `dispatchio.cli.app:app`.
- Create root Typer app.
- Import sub-app modules and register with `add_typer(...)`.
- Register root commands from `root.py` using `@app.command()`.
- Define `handle_cli_errors` decorator (see Error handling section).
- Install Rich tracebacks.
- Declare the root app callback with the `--no-color` flag (see Rich UX enhancements).
- Keep `if __name__ == "__main__": app()`.
- Import sub-apps with aliases: `from dispatchio.cli.record import app as record_app`.
- No other business logic.

`main.py` is **deleted**. Update `pyproject.toml` entry point:

```toml
[project.scripts]
dispatchio = "dispatchio.cli.app:app"
```

Update `dispatchio/__main__.py` to import from `app`:

```python
from dispatchio.cli.app import app
```

### errors.py

- Define one exception class:
  ```python
  class CliUserError(Exception):
      """Raised for user-facing CLI failures. Caught by @handle_cli_errors."""
  ```
- No other logic in this module.

### root.py

- Contains only root command functions decorated with `@app.command()`:
  - `tick`
  - `run` — entry-point form: `dispatchio run MODULE:FUNCTION`
  - `run_script` — file form: `dispatchio run-script FILE FUNCTION`
  - `status`
  - `ticks`
  - `cancel`
- Uses helpers from `loaders.py`, `options.py`, and `output.py`.
- Raises `CliUserError` for user-facing failures; never calls `sys.exit` directly.

#### run vs run-script split

The old single `run` command had two incompatible modes controlled by runtime
if/elif logic with no parse-time validation. They are now two separate commands:

| Command | Form | Required args |
|---------|------|---------------|
| `dispatchio run MODULE:FUNCTION` | module import | `EntryPointArgument` (positional, required) |
| `dispatchio run-script FILE FUNCTION` | file load | `ScriptPathArgument`, `FunctionNameArgument` (both positional, required) |

Both accept `--job-name NAME` as an optional override.
Neither accepts the other's arguments — the ambiguity is eliminated at parse time.
Both are machine-facing subprocess entry points used by PythonJob executors.

`run_script` must use `@app.command("run-script")` explicitly so the CLI name
uses a hyphen while the Python function name uses an underscore.

### record.py / retry.py / ctx.py / graph.py

- Each module defines `app = typer.Typer(help="...")` at the top.
- Module-local types and enums stay in their own module. Specifically:
  - `RetryListView` (the `attempts` / `requests` enum for `retry list --filter`) lives
    in `retry.py`, not in `options.py`, since it is used only by that one command.
- Each module owns its command functions decorated with `@app.command(...)`.
- In `app.py`, import with aliases, e.g.:
  ```python
  from dispatchio.cli.record import app as record_app
  root_app.add_typer(record_app, name="record")
  ```
- No cross-imports between command modules.
- Shared functionality only via `loaders.py`, `options.py`, and `output.py`.
- Each module also declares a `@app.callback()` with its help text (required by Typer
  for the help string to appear on the sub-command group).

Note: `record.py` contains exactly one command (`record set`). This is intentional —
the module exists for structural consistency, not because there are multiple record
commands. Do not add grouping, nesting, or extra abstractions.

`record set` is a destructive operation — it must include `yes: YesOption = False`
and call `output.confirm(...)` before writing, consistent with `cancel`.

### loaders.py

- `load_orchestrator(path: str) -> Orchestrator`
- `load_store_from_context(context_name: str | None) -> SQLAlchemyStateStore`
- `resolve_tick_log_store(context_name: str | None) -> FilesystemTickLogStore`
- All three return the requested object or raise `CliUserError` — they never
  return `None`. Callers do not need to check the return value for None.
- No `sys.exit` calls; no direct output.
- `_resolve_orchestrator_path` is removed — env var resolution is now handled by
  Typer's `envvar=` parameter in `options.py` (see below).

### options.py

Defines all shared `Annotated` aliases. Command functions use these as parameter
type annotations. Each alias is a type alias, not a value.

```python
from __future__ import annotations
from pathlib import Path
from typing import Annotated, Optional
import typer

# Shared option aliases

OrchestratorOption = Annotated[
    Optional[str],
    typer.Option(
        "--orchestrator", "-o",
        envvar="DISPATCHIO_ORCHESTRATOR",
        help="module:attribute path to an Orchestrator, e.g. myproject.jobs:orchestrator",
        show_default=False,
    ),
]

ContextOption = Annotated[
    Optional[str],
    typer.Option(
        "--context", "-c",
        help="Named context from ~/.dispatchio/contexts.json. Run 'dispatchio context list'.",
        show_default=False,
    ),
]

ReferenceTimeOption = Annotated[
    Optional[str],
    typer.Option(
        "--reference-time", "-t",
        help="ISO-8601 datetime to use as the tick reference time. Defaults to now (UTC).",
        show_default=False,
    ),
]

RunIdOption = Annotated[
    Optional[str],
    typer.Option("--run-id", "-r", help="Filter by logical run ID.", show_default=False),
]

# Required run-id (no Optional) — used in cancel, retry create where run-id is mandatory
RunIdRequiredOption = Annotated[
    str,
    typer.Option("--run-id", "-r", help="Logical run ID."),
]

JobOption = Annotated[
    Optional[str],
    typer.Option("--job", "-j", help="Filter by job name.", show_default=False),
]

# Required job (no Optional) — used in cancel where job is mandatory
JobRequiredOption = Annotated[
    str,
    typer.Option("--job", "-j", help="Job name."),
]

# Repeatable jobs list — used in retry create
JobsOption = Annotated[
    Optional[list[str]],
    typer.Option(
        "--job", "-j",
        help="Job name (repeatable). If omitted, retries all failed jobs.",
        show_default=False,
    ),
]

AttemptOption = Annotated[
    Optional[int],
    typer.Option("--attempt", "-a", help="Attempt number.", show_default=False),
]

LimitOption = Annotated[
    int,
    typer.Option("--limit", "-n", help="Max rows to show.", show_default=True),
]

ReasonOption = Annotated[
    Optional[str],
    typer.Option("--reason", help="Reason string.", show_default=False),
]

OperatorOption = Annotated[
    str,
    typer.Option("--operator", help="Name/ID of the operator.", show_default=True),
]

StatusFilterOption = Annotated[
    Optional[Status],  # Status imported from dispatchio.models
    typer.Option("--status", help="Filter by status.", case_sensitive=False, show_default=False),
]

# Skip confirmation prompt — used on destructive commands (cancel, record set)
YesOption = Annotated[
    bool,
    typer.Option("--yes", "-y", help="Skip confirmation prompt.", show_default=False),
]

# Shared argument aliases

EntryPointArgument = Annotated[
    str,
    typer.Argument(help="module:function entry point, e.g. mypackage.jobs:run_ingest"),
]

ScriptPathArgument = Annotated[
    Path,
    typer.Argument(callback=_validate_existing_path, help="Path to a Python script file."),
]

FunctionNameArgument = Annotated[
    str,
    typer.Argument(help="Function name within the script."),
]

GraphPathArgument = Annotated[
    Path,
    typer.Argument(callback=_validate_existing_path, help="Path to graph artifact file."),
]
```

`_validate_existing_path` is a module-private callback defined in `options.py`:

```python
def _validate_existing_path(value: Path) -> Path:
    if not value.exists():
        raise typer.BadParameter(f"Path does not exist: {value}")
    return value
```

`OrchestratorOption` now includes `envvar="DISPATCHIO_ORCHESTRATOR"`, so Typer handles
env var lookup automatically. The `_resolve_orchestrator_path` helper in the old
`main.py` is **removed**. Commands receive the resolved value directly as their
`orchestrator` parameter.

### output.py (Rich-first)

- Owns all output behavior.
- Module-level singleton:
  ```python
  from rich.console import Console
  console = Console(highlight=False)
  error_console = Console(stderr=True, highlight=False)
  ```
- Provides a small API used by command modules:
  - `print_error(message: str) -> None` — writes to `error_console`
  - `print_success(message: str) -> None`
  - `print_warning(message: str) -> None`
  - `print_records(records: list[AttemptRecord]) -> None` — Rich `Table`
  - `print_tick_result(result: TickResult) -> None` — styled action list in a `Panel`
  - `print_tick_summary(record, *, detail: bool = False) -> None`
  - `print_retry_requests(requests: list[RetryRequest]) -> None` — Rich `Table`
  - `print_context_list(entries, current: str | None) -> None` — Rich `Table`
  - `print_json(data: str) -> None` — delegates to `console.print_json`
  - `confirm(message: str) -> bool` — Rich `Confirm.ask`; returns `True` if confirmed
- Color/styling constants and `_ACTION_ICONS` dict live here.
- `print_error` does **not** call `sys.exit`; callers raise `CliUserError` and
  `@handle_cli_errors` calls `print_error` before exiting.

## Error handling strategy

### errors.py

```python
class CliUserError(Exception):
    """Raised for user-facing CLI failures. Caught by @handle_cli_errors."""
```

### @handle_cli_errors decorator (defined in app.py)

All root commands and sub-app commands are decorated with `@handle_cli_errors`.
The decorator is defined once in `app.py` and imported by each command module.

```python
import functools
from dispatchio.cli.errors import CliUserError
from dispatchio.cli import output

def handle_cli_errors(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except CliUserError as e:
            output.print_error(str(e))
            raise typer.Exit(code=1)
    return wrapper
```

Usage in every command:

```python
@app.command("tick")
@handle_cli_errors
def tick(*, orchestrator: OrchestratorOption = None, ...) -> None:
    ...
```

`loaders.py` raises `CliUserError`; commands raise `CliUserError` for their own
validation. `@handle_cli_errors` is the single catch point; nothing else calls
`sys.exit` or `typer.Exit` directly except via this decorator.

Unexpected exceptions (not `CliUserError`) are intentionally left unhandled so
stack traces remain visible during alpha. Rich tracebacks (installed in `app.py`)
make these readable — see Rich UX enhancements section.

## Rich Console and testability

### Problem

Rich's `Console` singleton writes directly to `sys.stdout`/`sys.stderr`. Typer's
`CliRunner` (used in tests) captures output via Click's stream-redirection mechanism,
which does **not** intercept Rich's direct writes. This means `result.output` from
`CliRunner.invoke(...)` will be empty when commands use Rich.

### Solution: two-tier testing

- **Exit codes and side effects** (DB record changes, file writes): use `CliRunner`
  as before. `result.exit_code` is reliable.
- **Output content**: patch `dispatchio.cli.output.console` with a
  `Console(record=True, no_color=True)` before invoking the command, then call
  `console.export_text()` to assert content.

### Pytest fixture pattern

```python
# tests/conftest.py
import pytest
from unittest.mock import patch
from rich.console import Console

@pytest.fixture
def rich_output():
    """Patches the CLI output console and returns it for assertions."""
    c = Console(record=True, no_color=True, width=200)
    with patch("dispatchio.cli.output.console", c):
        yield c

# Usage in tests:
def test_status_shows_empty_message(rich_output):
    result = runner.invoke(app, ["status", "--context", "..."])
    assert result.exit_code == 0
    assert "No records found" in rich_output.export_text()
```

`error_console` can be patched the same way when testing error output:

```python
with patch("dispatchio.cli.output.error_console", Console(record=True, no_color=True)):
    ...
```

## Rich UX enhancements

Typer remains the command framework. Rich only handles presentation.

Add `rich>=13` to the `[cli]` optional dependency group in `pyproject.toml` and
to `[dev]` extras.

### Tables (replace fixed-width string formatting)

Replace all `typer.echo(...)` and `typer.style(...)` calls with `output.py` helpers.
Use Rich `Table` for all tabular output:

- `status` → `print_records` — columns: JOB, RUN_ID, STATUS (coloured), ATTEMPT, COMPLETED
- `retry list --filter attempts` → `print_records`
- `retry list --filter requests` → `print_retry_requests` — columns: REQUESTED_AT, BY, RUN_ID, JOBS
- `context list` → `print_context_list` — columns: (active marker), NAME, CONFIG PATH, DESCRIPTION

### Panel for tick results

Replace the plain `─` separator + line-per-action output with a Rich `Panel`
containing a styled list. The panel title is the reference time; action icons and
colours come from `_ACTION_ICONS` and `_STATUS_COLOURS` in `output.py`.

### Syntax-highlighted JSON

`graph schema` uses `console.print_json(schema_str)` instead of `typer.echo`.
This gives free syntax highlighting when the terminal supports colour.

### Loading spinners

Use `with console.status("...")` for any command that performs I/O before producing
output. This makes latency feel intentional rather than frozen:

| Command | Spinner label |
|---------|---------------|
| `tick` | `"Loading orchestrator…"` |
| `status` | `"Loading state…"` |
| `ticks` | `"Loading tick log…"` |
| `retry create` | `"Loading orchestrator…"` |
| `cancel` | `"Loading state…"` |

### Confirmation prompts for destructive operations

`cancel` and `record set` modify state and are hard to undo. Both commands gain
a `--yes / -y` flag (via `YesOption` from `options.py`). When `--yes` is not
passed, the command calls `output.confirm(...)` before writing:

```python
# cancel
if not yes and not output.confirm(f"Cancel {job}[{run_id}] attempt {rec.attempt}?"):
    raise typer.Exit()

# record set
if not yes and not output.confirm(f"Set {job_name}[{run_id}] → {new_status.value}?"):
    raise typer.Exit()
```

`output.confirm` uses `rich.prompt.Confirm.ask`. Scripts and executors pass
`--yes` to suppress the prompt.

### --no-color flag and NO_COLOR env var

Rich respects the `NO_COLOR` environment variable natively. Additionally, expose
`--no-color` as a root app callback option in `app.py` for explicit CLI control:

```python
@app.callback()
def callback(
    no_color: Annotated[bool, typer.Option("--no-color", help="Disable colour output.", is_eager=True)] = False,
) -> None:
    if no_color:
        output.console = Console(highlight=False, no_color=True)
        output.error_console = Console(stderr=True, highlight=False, no_color=True)
```

`is_eager=True` ensures this runs before any command logic.

### Rich tracebacks (alpha)

Installed once in `app.py` for syntax-highlighted, locals-annotated tracebacks
during the alpha phase:

```python
from rich.traceback import install as _install_rich_tb
_install_rich_tb(show_locals=False)
```

## Migration sequence

1. Create `errors.py` with `CliUserError`.
2. Create module skeletons (`app.py`, `root.py`, `options.py`, `loaders.py`,
   `output.py`, `record.py`, `retry.py`, `ctx.py`, `graph.py`) with only
   stubs/imports.
3. Populate `options.py` with all `Annotated` aliases (see above).
4. Populate `loaders.py` by moving the three loader functions from `main.py`,
   converting `_exit_with_error` calls to `raise CliUserError(...)`.
5. Populate `output.py` with the Rich console singleton, all output functions,
   and `confirm()`.
6. Move root commands into `root.py`, applying `@handle_cli_errors`, swapping
   output calls to `output.py` helpers, and adding `--yes` + `confirm()` to
   `cancel` and `record set`.
7. Move each sub-app into its own module (`record.py`, `retry.py`, `ctx.py`,
   `graph.py`), following the same pattern.
8. Wire everything together in `app.py`: sub-app registration, `handle_cli_errors`
   definition, root callback with `--no-color`, Rich traceback install.
9. Update `pyproject.toml` entry point to `dispatchio.cli.app:app`.
   Update `dispatchio/__main__.py` to import from `dispatchio.cli.app`.
   Delete `dispatchio/cli/main.py`.
10. Add `conftest.py` with `rich_output` fixture; update existing tests to use
    the two-tier approach.
11. Verify `python -m dispatchio --help` and `dispatchio --help` both work.

## Testing plan

### Existing tests

- All existing tests will need to be updated to use the `rich_output` fixture for
  output content assertions. Exit code assertions remain unchanged.

### Tests to add

- Root help lists all expected commands: `tick`, `run`, `run-script`, `status`, `ticks`, `cancel`, `record`, `retry`, `context`, `graph`.
- `@handle_cli_errors` returns exit code 1 and emits error text on `CliUserError`.
- Rich table outputs include expected content fields for:
  - `status`
  - `retry list --filter attempts`
  - `retry list --filter requests`
  - `context list`
- Regression tests for:
  - `run` command validation (entry point form and script form)
  - Missing orchestrator errors
  - Graph validation failures

## Breaking changes accepted in alpha

- Output formatting changes completely (Rich tables/colours).
- `typer.echo`/`typer.style` patterns are removed.
- `dispatchio.cli.main` is deleted; entry point moves to `dispatchio.cli.app`.
- `dispatchio run` no longer accepts `--script`/`--function`; use `dispatchio run-script` instead.
- `cancel` and `record set` now prompt for confirmation unless `--yes` is passed.

## Acceptance criteria

- CLI code split into dedicated modules with clear ownership.
- No cyclic imports.
- All command output uses Rich through shared output helpers.
- `@handle_cli_errors` decorator is the single exit point for user-facing errors.
- `dispatchio.cli.main` is deleted; `pyproject.toml` points to `dispatchio.cli.app:app`.
- `--no-color` flag and `NO_COLOR` env var suppress all colour output.
- `cancel` and `record set` prompt for confirmation; `--yes` suppresses it.
- Entry points work:
  - `python -m dispatchio --help`
  - `dispatchio --help`
- Test suite passes after updates.

## Implementation notes

- Keep this refactor focused on structure/output only.
- Do not change orchestration semantics in this pass.

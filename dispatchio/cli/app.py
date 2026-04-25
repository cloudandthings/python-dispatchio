from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console
from rich.traceback import install as _install_rich_tb

from dispatchio.cli import output


_install_rich_tb(show_locals=False)

app = typer.Typer(
    help="Dispatchio - lightweight tick-based batch job orchestrator.",
    no_args_is_help=True,
)


@app.callback()
def callback(
    no_color: Annotated[
        bool,
        typer.Option(
            "--no-color",
            help="Disable colour output.",
            is_eager=True,
        ),
    ] = False,
) -> None:
    if no_color:
        output.console = Console(highlight=False, no_color=True)
        output.error_console = Console(stderr=True, highlight=False, no_color=True)


from dispatchio.cli import root as _root  # noqa: F401,E402
from dispatchio.cli.ctx import app as ctx_app  # noqa: E402
from dispatchio.cli.graph import app as graph_app  # noqa: E402
from dispatchio.cli.record import app as record_app  # noqa: E402
from dispatchio.cli.rename import event_app, job_app, namespace_app  # noqa: E402
from dispatchio.cli.retry import app as retry_app  # noqa: E402
from dispatchio.cli.sandbox import app as sandbox_app  # noqa: E402

app.add_typer(record_app, name="record")
app.add_typer(retry_app, name="retry")
app.add_typer(ctx_app, name="context")
app.add_typer(graph_app, name="graph")
app.add_typer(job_app, name="job")
app.add_typer(namespace_app, name="namespace")
app.add_typer(event_app, name="event")
app.add_typer(sandbox_app, name="sandbox")


if __name__ == "__main__":
    app()

"""
Hello World worker functions — with @job decorators.

Decorating with @job co-locates the job definition with the function, so you
don't need a separate jobs.py for simple cases.  Run the whole file with:

    dispatchio run-file examples/hello_world/my_work.py

To run for an explicit run key (e.g. today's date or an event identifier):

    dispatchio run-file examples/hello_world/my_work.py --run-key D20260423

If a dependency isn't satisfied yet, the downstream job is held back.
Override that with --ignore-dependencies.

For complex pipelines (cross-file deps, dynamic registration, graph loading)
the explicit jobs.py approach is still available — see jobs.py in this directory.
"""

import time

from dispatchio import DAILY, job


@job(cadence=DAILY)
def hello_world(run_key: str) -> None:
    print(f"Hello, World! Running for {run_key}.")
    time.sleep(0.3)


@job(cadence=DAILY, depends_on=hello_world)
def goodbye_world(run_key: str) -> None:
    print(f"Goodbye, World! Wrapping up {run_key}.")
    time.sleep(0.3)

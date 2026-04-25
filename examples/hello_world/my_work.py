"""
Hello World worker functions — with @job decorators.

Decorating with @job creates a simple job definition around your function.

Run the whole file with:

    dispatchio run-file examples/hello_world/my_work.py

To run for an explicit run key (e.g. today's date or an event identifier):

    dispatchio run-file examples/hello_world/my_work.py --run-key D20260423

If a dependency isn't satisfied yet, the downstream job is held back.
Override that with --ignore-dependencies.

Cadence can be supplied to the @job decorators below — in this example,
dispatchio.toml sets default_cadence = "daily" so it applies automatically.

For complex pipelines (cross-file deps, dynamic registration, graph loading)
the explicit jobs.py approach is still available — see examples/hello_world/jobs.py
"""

import time

from dispatchio import job


# cadence is omitted — default_cadence=DAILY from dispatchio.toml applies.
@job()
def hello_world(run_key: str) -> None:
    print(f"Hello, World! Running for {run_key}.")
    time.sleep(0.3)


# cadence is omitted — default_cadence=DAILY from dispatchio.toml applies.
@job(depends_on=hello_world)
def goodbye_world(run_key: str) -> None:
    print(f"Goodbye, World! Wrapping up {run_key}.")
    time.sleep(0.3)

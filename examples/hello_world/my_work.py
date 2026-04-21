"""
Hello World worker functions.

Pure Python callables — no Dispatchio imports needed.
Dispatchio handles the job lifecycle (run_key resolution, completion events).
Entry-point jobs run via `dispatchio run`; script-backed jobs run via `dispatchio run-script`.

Each function receives only what its signature declares. Declare `run_key` to
receive the run key, `job_name` to receive the job name, both, or neither.
"""

import time


def hello_world(run_key: str) -> None:
    print(f"Hello, World! Running for {run_key}.")
    time.sleep(0.3)


def goodbye_world(run_key: str) -> None:
    print(f"Goodbye, World! Wrapping up {run_key}.")
    time.sleep(0.3)

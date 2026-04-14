"""
Hello World worker functions.

Pure Python callables — no Dispatchio imports needed.
`dispatchio run` handles the job lifecycle (run_id resolution, completion events).

Each function receives only what its signature declares. Declare `run_id` to
receive the run ID, `job_name` to receive the job name, both, or neither.
"""

import time


def hello_world(run_id: str) -> None:
    print(f"Hello, World! Running for {run_id}.")
    time.sleep(0.3)


def goodbye_world(run_id: str) -> None:
    print(f"Goodbye, World! Wrapping up {run_id}.")
    time.sleep(0.3)

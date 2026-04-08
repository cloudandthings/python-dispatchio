"""
Hello World worker functions.

Pure Python callables — no Dispatchio imports needed.
`dispatchio run` handles the job lifecycle (run_id resolution, completion events).

In a real project each job would typically live in its own module or be part
of an installed package. Here both functions share one file for simplicity.
"""

import time


def hello_world(run_id: str) -> None:
    print(f"Hello, World! Running for {run_id}.")
    time.sleep(0.3)


def goodbye_world(run_id: str) -> None:
    print(f"Goodbye, World! Wrapping up {run_id}.")
    time.sleep(0.3)

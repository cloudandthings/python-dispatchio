"""
Worker functions for the SubprocessJob example.

Unlike PythonJob, SubprocessJob does NOT get the dispatchio harness
automatically — the worker script must call run_job() itself.

The executor injects DISPATCHIO_RUN_ID and DISPATCHIO_DROP_DIR as env vars
(see jobs.py); run_job() picks them up and handles DONE/ERROR reporting.

Usage (dispatched by SubprocessJob in jobs.py):
    python my_work.py generate
    python my_work.py summarize
"""

import sys

from dispatchio.worker.harness import run_job


def generate(run_id: str) -> None:
    """Simulate generating data — always succeeds."""
    items = list(range(10))
    print(f"Generated {len(items)} items for run_id={run_id}")


def summarize(run_id: str) -> None:
    """Deliberately fail to demonstrate error handling and retries."""
    print(f"Attempting to summarize {run_id}...")
    raise RuntimeError("Data source unavailable — demonstrating error handling")


_DISPATCH = {"generate": generate, "summarize": summarize}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in _DISPATCH:
        print(f"Usage: python my_work.py <{'|'.join(_DISPATCH)}>", file=sys.stderr)
        sys.exit(1)
    fn_name = sys.argv[1]
    run_job(fn_name, _DISPATCH[fn_name])

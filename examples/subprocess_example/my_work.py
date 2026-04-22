"""
Worker functions for the SubprocessJob example.

Unlike PythonJob, SubprocessJob does NOT get the dispatchio harness
automatically — the worker script must call run_job() itself.

The orchestrator injects receiver configuration as env vars:
  - DISPATCHIO_CONFIG

run_job() uses these to auto-configure the reporter. You can also manually
use get_reporter() for explicit control:

    from dispatchio.completion import get_reporter
    reporter = get_reporter("my_job")
    reporter.report_success(run_key, metadata={"items": 100})

Usage (dispatched by SubprocessJob in jobs.py):
    python my_work.py generate
    python my_work.py summarize
"""

import sys

from dispatchio.worker.harness import run_job


def generate(run_key: str) -> None:
    """Simulate generating data — always succeeds."""
    items = list(range(10))
    print(f"Generated {len(items)} items for run_key={run_key}")
    # run_job() automatically reports success with metadata


def summarize(run_key: str) -> None:
    """Deliberately fail to demonstrate error handling and retries."""
    print(f"Attempting to summarize {run_key}...")
    raise RuntimeError("Data source unavailable — demonstrating error handling")
    # run_job() automatically reports error and raises SystemExit(1)


_DISPATCH = {"generate": generate, "summarize": summarize}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in _DISPATCH:
        print(f"Usage: python my_work.py <{'|'.join(_DISPATCH)}>", file=sys.stderr)
        sys.exit(1)
    fn_name = sys.argv[1]
    run_job(fn_name, _DISPATCH[fn_name])

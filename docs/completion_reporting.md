# Completion Reporting Abstraction

## Overview

Dispatchio provides a clean abstraction for jobs to report their completion status. The abstraction is **backend-agnostic** — your job code doesn't need to know whether the orchestrator is using filesystem drops, SQS, or custom backends.

## Architecture

```
┌─────────────────────────┐
│  Job Code               │
│  ├─ get_reporter()      │ ← unified interface
│  └─ reporter.report_*() │
└────────────┬────────────┘
             │ environment vars: DISPATCHIO_RECEIVER__*
             ↓
┌────────────────────────────────┐
│  Orchestrator Config           │
│  ├─ ReceiverSettings           │
│  ├─ backend = "filesystem"     │
│  │   or "sqs"                  │
│  │   or custom                 │
│  └─ injected as env vars       │
└────────────┬───────────────────┘
             ↓
┌────────────────────────────────┐
│  Completion Receiver           │
│  ├─ FilesystemReceiver         │
│  ├─ SQSReceiver                │
│  └─ Custom implementations     │
└────────────────────────────────┘
```

## Using get_reporter()

The simplest approach — designed for Python jobs using the `run_job()` harness:

### With run_job() harness (recommended)

```python
# my_job.py
from dispatchio.worker.harness import run_job
from dispatchio.completion import get_reporter

def main(run_id: str) -> None:
    reporter = get_reporter("my_job")
    
    print(f"Processing {run_id}...")
    try:
        # do work...
        rows = do_ingest()
        
        # report success with metadata
        reporter.report_success(run_id, metadata={"rows": rows})
    except Exception as exc:
        reporter.report_error(run_id, str(exc))
        raise

if __name__ == "__main__":
    run_job("my_job", main)
```

Submit as a PythonJob:
```python
Job(
    name="my_job",
    executor=PythonJob(script="my_job.py", function="main"),
)
```

The orchestrator automatically injects the correct receiver configuration via environment variables.

### Without run_job() harness

```python
# manual_completion.py
from dispatchio.completion import get_reporter

def main(run_id: str) -> None:
    reporter = get_reporter("my_job")
    
    print(f"Processing {run_id}...")
    try:
        # do work...
        result = do_something()
        reporter.report_success(run_id, metadata={"result": result})
    except Exception as exc:
        reporter.report_error(run_id, f"Failed: {exc}")

if __name__ == "__main__":
    import sys
    run_id = sys.argv[1]
    main(run_id)
```

Submit as a SubprocessJob:
```python
Job(
    name="my_job",
    executor=SubprocessJob(command=["python", "manual_completion.py", "{run_id}"]),
)
```

## Using build_reporter()

For advanced use cases where you want to build a reporter directly from config:

```python
from dispatchio.config import ReceiverSettings
from dispatchio.completion import build_reporter

# Build from settings object
receiver_cfg = ReceiverSettings(
    backend="filesystem",
    drop_dir="/tmp/drops",
)
reporter = build_reporter(receiver_cfg)

if reporter:
    reporter.report("my_job", "20250115", Status.DONE)
```

## CompletionReporter Interface

The high-level interface your code interacts with:

```python
class CompletionReporter(Protocol):
    def report_success(
        self,
        run_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Report successful completion with optional metadata."""
        ...

    def report_error(self, run_id: str, error_reason: str) -> None:
        """Report failure with error message."""
        ...

    def report_running(self, run_id: str) -> None:
        """Send heartbeat (for long-running jobs)."""
        ...
```

## Environment Variable Configuration

When `orchestrator_from_config()` creates an orchestrator, it automatically injects receiver configuration as environment variables for spawned jobs:

| Config Backend | Environment Variables |
|---|---|
| `filesystem` | `DISPATCHIO_RECEIVER__BACKEND=filesystem`<br>`DISPATCHIO_RECEIVER__DROP_DIR=/path/to/drops`<br>`DISPATCHIO_DROP_DIR=/path/to/drops` (legacy) |
| `sqs` | `DISPATCHIO_RECEIVER__BACKEND=sqs`<br>`DISPATCHIO_RECEIVER__QUEUE_URL=https://...`<br>`DISPATCHIO_RECEIVER__REGION=us-east-1` |
| Custom | `DISPATCHIO_RECEIVER__BACKEND=custom_type` + backend-specific vars |

Jobs use `get_reporter()` which reads these environment variables automatically.

## Configuration Examples

### Local development (filesystem)

```toml
# dispatchio.toml
[dispatchio.receiver]
backend = "filesystem"
drop_dir = ".dispatchio/completions"
```

Jobs automatically inject:
```bash
DISPATCHIO_RECEIVER__BACKEND=filesystem
DISPATCHIO_RECEIVER__DROP_DIR=.dispatchio/completions
```

### AWS deployment (SQS)

```toml
# dispatchio.toml
[dispatchio.receiver]
backend = "sqs"
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789/dispatchio-events"
region = "us-east-1"
```

Jobs automatically inject:
```bash
DISPATCHIO_RECEIVER__BACKEND=sqs
DISPATCHIO_RECEIVER__QUEUE_URL=https://sqs.us-east-1.amazonaws.com/...
DISPATCHIO_RECEIVER__REGION=us-east-1
```

### No receiver (state store only)

```toml
# dispatchio.toml
[dispatchio.receiver]
backend = "none"
```

Jobs can still call `get_reporter()` — it returns a no-op reporter that logs warnings.

## Migration Guide

If you're currently using manual JSON file writing or custom reporter setup:

### Before

```python
# Old way — direct file writing
import json, pathlib

drop = pathlib.Path(".dispatchio/completions")
drop.mkdir(exist_ok=True)
(drop / f"my_job__{run_id}__done.json").write_text(json.dumps({
    "job_name": "my_job",
    "run_id": run_id,
    "status": "done",
}))
```

### After

```python
# New way — clean abstraction
from dispatchio.completion import get_reporter

reporter = get_reporter("my_job")
reporter.report_success(run_id)
```

Benefits:
- ✅ Automatically detects backend from config
- ✅ No hardcoded paths or backend details
- ✅ Works for filesystem, SQS, and future backends
- ✅ Better error handling
- ✅ Metadata support built-in
- ✅ Easier to test

## Testing

For unit tests, you can manually inject a reporter:

```python
from dispatchio.completion import _CompletionReporterAdapter
from dispatchio.worker.reporter.filesystem import FilesystemReporter

def test_my_job(tmp_path):
    # Create a temporary reporter
    fs_reporter = FilesystemReporter(tmp_path)
    reporter = _CompletionReporterAdapter("my_job", fs_reporter)
    
    # Call your job logic with the reporter
    # ... test code ...
    
    # Verify completion event was written
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
```

Or test without a reporter:

```python
from dispatchio.completion import _CompletionReporterAdapter

def test_my_job_logic():
    # No-op reporter (logs warnings but doesn't fail)
    reporter = _CompletionReporterAdapter("my_job", None)
    
    # Test your job logic
    # ... test code ...
```

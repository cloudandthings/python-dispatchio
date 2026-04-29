# Job Run Key Rename - Impact Assessment

## Purpose

This document evaluates the impact of renaming the current job-scoped
`job_run_key` concept to a clearer name.

The primary motivation is that Dispatchio is now introducing a separate
orchestrator-scoped run concept, named `run_key`. Once both scopes exist, `job_run_key` becomes less clear:

- it uses `id` for a value that is not an opaque identity or table primary key
- it overlaps semantically with the new orchestrator run concept
- it is precise but not especially operator-friendly

This document focuses on the rename impact, not the orchestrator-run feature as
such.

---

## Decision

In effect the rename is:

- `job_run_key` -> `job_run_key`

The complementary naming scheme is:

- `orchestrator_run_id` = opaque persisted primary key for an orchestrator run row
- `run_key` = orchestrator-scoped logical run key
- `job_run_key` = job-scoped logical run key used to group attempts
- `dispatchio_attempt_id` = concrete execution identity for one submitted attempt

This gives a clean, scope-aware vocabulary:

- outer orchestration key
- inner job-state key
- concrete attempt id

### Why `job_run_key` is preferred

- `job_` makes the scope explicit.
- `run_` preserves the operator-facing concept already present in retries,
  dependencies, and completion reporting.
- `key` is more accurate than `id` because the value is derived and used for
  grouping rather than acting as an opaque surrogate identifier.
- it stays distinct from `orchestrator_run_id` and `run_key`.

---

## Rename Matrix

Recommended rename matrix:

| Current | Proposed | Scope | Notes |
|---|---|---|---|
| `job_run_key` | `job_run_key` | Job state | Main rename |
| `resolve_job_run_key()` | `resolve_job_run_key()` | Run resolution helper | Better aligned with new terminology |
| `run_id.py` | `run_key_resolution.py` | Module | Optional but recommended |
| `CompletionEvent.job_run_key` | `CompletionEvent.job_run_key` | Completion payload | Backward compat alias is unnecessary; we are in alpha |
| `RetryRequest.job_run_key` | `RetryRequest.job_run_key` | Retry audit | Keeps retry scope job-level |
| `get_job_run_key()` | `get_job_run_key()` | Receiver helper | Rename to match payload field |
| `--job-run-key` for job-targeting commands | `--job-run-key` | CLI | Distinguishes job-level targeting from orchestrator run selection |
| `DISPATCHIO_JOB_RUN_KEY` | `DISPATCHIO_JOB_RUN_KEY` | Worker env | Needed as we are doing a full cleanup |

Not proposed for rename:

- `dispatchio_attempt_id`
- `orchestrator_run_id`
- orchestrator `run_key`

---

## Scope Of Impact

The rename touches nearly every layer of the system.

### 1. Core domain models

Directly affected models include:

- `AttemptRecord`
- `DeadLetterRecord`
- `RetryRequest`
- `CompletionEvent`
- any helper types carrying resolved per-job run identity

Representative files:

- [dispatchio/models.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/models.py)
- [dispatchio/receiver/base.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/receiver/base.py)
- [dispatchio/orchestrator.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/orchestrator.py)

Impact level: high.

Reason:

- these are canonical shared types used across orchestrator logic, state, CLI,
  tests, and AWS integration

### 2. State store protocol and persistence schema

Directly affected surfaces:

- `StateStore.get_latest_attempt(job_name, job_run_key)`
- `StateStore.list_attempts(job_run_key=...)`
- `StateStore.list_retry_requests(job_run_key=...)`
- SQLAlchemy row column names and constraints

Representative files:

- [dispatchio/state/base.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/state/base.py)
- [dispatchio/state/sqlalchemy_.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/state/sqlalchemy_.py)

Impact level: very high.

Reason:

- both Python API and underlying database column names are affected
- unique constraints and index names would ideally be renamed too

### 3. Orchestrator internals

The orchestrator is one of the heaviest rename surfaces because it resolves,
stores, compares, logs, retries, and cancels using this field everywhere.

Representative file:

- [dispatchio/orchestrator.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/orchestrator.py)

Impact level: very high.

Reason:

- helper names
- local variables
- error messages
- alerts
- dependency evaluation
- completion correlation
- retry and cancel workflows

### 4. Executors and worker runtime contract

Affected areas:

- executor payload interpolation
- worker environment variables
- worker harness argument parsing
- reporter method signatures and emitted completion payloads

Representative files:

- [dispatchio/executor/base.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/executor/base.py)
- [dispatchio/executor/python_.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/executor/python_.py)
- [dispatchio/executor/subprocess_.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/executor/subprocess_.py)
- [dispatchio/worker/harness.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/worker/harness.py)
- [dispatchio/worker/reporter/base.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/worker/reporter/base.py)
- [dispatchio/worker/reporter/filesystem.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/worker/reporter/filesystem.py)

Impact level: high.

Reason:

- this is the contract boundary between orchestrator and user code
- naming here affects user scripts, not just internal code

### 5. CLI surface

Current job-level commands still talk in terms of `run_id` rather than
`job_run_key`, but they semantically target the same concept.

Affected areas:

- option names
- help text
- printed column names
- filter semantics

Representative files:

- [dispatchio/cli/options.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/cli/options.py)
- [dispatchio/cli/root.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/cli/root.py)
- [dispatchio/cli/retry.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/cli/retry.py)
- [dispatchio/cli/record.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/cli/record.py)
- [dispatchio/cli/output.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/cli/output.py)

Impact level: medium to high.

Reason:

- user-facing terminology needs to become scope-explicit once orchestrator run
  commands are introduced

### 6. AWS extension surfaces

Affected areas:

- payloads passed to AWS backends
- completion handler payload translation
- execution names and response shaping

Representative files:

- [dispatchio_aws/executor/base.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio_aws/executor/base.py)
- [dispatchio_aws/worker/eventbridge_handler.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio_aws/worker/eventbridge_handler.py)
- [dispatchio_aws/reporter/sqs.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio_aws/reporter/sqs.py)
- [dispatchio_aws/worker/lambda_handler.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio_aws/worker/lambda_handler.py)

Impact level: medium.

Reason:

- the extension layer is smaller, but it sits on integration boundaries and may
  need deliberate compatibility choices

### 7. Public documentation, examples, and cookbook content

This is a significant rename surface.

Affected categories:

- conceptual docs
- cookbook content
- example code
- archived design notes that may still be referenced historically

Representative files:

- [docs/retries_attempts_audit.md](/Users/bjorn/github/cloudandthings/python-dispatchio/docs/retries_attempts_audit.md)
- [docs/completion_reporting.md](/Users/bjorn/github/cloudandthings/python-dispatchio/docs/completion_reporting.md)
- [docs/backfill_and_replay.md](/Users/bjorn/github/cloudandthings/python-dispatchio/docs/backfill_and_replay.md)
- [COOKBOOK.md](/Users/bjorn/github/cloudandthings/python-dispatchio/COOKBOOK.md)

Impact level: high.

Reason:

- a partial rename would create conceptual confusion immediately

### 8. Test suite

The test suite uses `job_run_key` extensively in fixtures, assertions, helper
constructors, and expected payloads.

Representative files:

- [tests/test_orchestrator.py](/Users/bjorn/github/cloudandthings/python-dispatchio/tests/test_orchestrator.py)
- [tests/test_state.py](/Users/bjorn/github/cloudandthings/python-dispatchio/tests/test_state.py)
- [tests/test_cli.py](/Users/bjorn/github/cloudandthings/python-dispatchio/tests/test_cli.py)
- [tests/test_worker.py](/Users/bjorn/github/cloudandthings/python-dispatchio/tests/test_worker.py)
- [tests/test_aws_eventbridge_handler.py](/Users/bjorn/github/cloudandthings/python-dispatchio/tests/test_aws_eventbridge_handler.py)

Impact level: high.

Reason:

- broad textual rename across assertions and payload fixtures

---

## What Actually Changes Semantically

The rename is not only textual.

It changes the conceptual contract from:

- a vaguely logical per-job run id

to:

- an explicit job-scoped run key

That matters because Dispatchio will now have two run scopes:

- orchestrator `run_key`
- job `job_run_key`

This is the real value of the rename. It is not cosmetic.

---

## Public API Direction

If the rename is adopted, use the following public-language cleanup.

### Python API

Prefer:

```python
orchestrator.manual_retry(job_name="ingest", job_run_key="20260418")
store.get_latest_attempt("ingest", job_run_key="20260418")
```

Avoid keeping mixed spellings such as:

```python
orchestrator.manual_retry(job_name="ingest", job_run_key="20260418")
orchestrator.resume_run(run_key="2026-04-18")
```

### CLI

Prefer:

- `--job-run-key` for commands that target job attempt history
- `--run-key` for orchestrator-run commands
- `--orchestrator-run-id` for persisted run row identities

This gives immediate visual separation in CLI usage.

### Worker and payload contracts

Prefer:

- `job_run_key` in internal typed payloads
- temporary `run_id` alias only if needed for compatibility

Alpha breakage is acceptable. A direct cutover is simpler and cleaner.

---

## Environment Variable Decision

This is the main compatibility fork in the road.

### Option A - full cleanup

Rename:

- `DISPATCHIO_JOB_RUN_KEY` -> `DISPATCHIO_JOB_RUN_KEY`

Pros:

- internally consistent
- no old naming left in the worker contract

Cons:

- more breakage for any existing user scripts or examples

### Option B - keep env var but rename typed fields

Keep:

- `DISPATCHIO_JOB_RUN_KEY`

Rename only typed model and API fields.

Pros:

- smaller integration blast radius

Cons:

- leaves one of the most visible old names in place
- creates a long-lived mismatch between env and code terminology

### Recommendation

Do the full cleanup.

Alpha is the easiest time to change the worker env contract.

---

## Database Schema Impact

If using the SQLAlchemy state store, a complete rename should include:

- row field names
- database column names
- unique constraint names
- index names where they embed the old term

Examples in [dispatchio/state/sqlalchemy_.py](/Users/bjorn/github/cloudandthings/python-dispatchio/dispatchio/state/sqlalchemy_.py):

- `job_run_key` column on attempt rows
- `uq_job_logical_run_attempt`

Because alpha allows breaking schema changes, the cleanest approach is not to
preserve old names in the database layer.

---

## Blast Radius Summary

Estimated impact by area:

| Area | Impact | Why |
|---|---|---|
| Domain models | High | Canonical shared types |
| State store protocol | Very high | API and schema both change |
| Orchestrator internals | Very high | Heavy use of the term |
| Executors and workers | High | Boundary with user code |
| CLI | Medium to high | User-facing terminology cleanup |
| AWS extension | Medium | Integration payloads |
| Docs and cookbook | High | Must stay conceptually consistent |
| Tests | High | Broad fixture and assertion updates |

Overall blast radius: high, but manageable and justified if the project is
committing to first-class orchestrator runs.

---

## Recommendation On Timing

This rename is desired, it should happen before or together with the new
orchestrator-run feature work.

Reasons:

- prevents a second naming migration shortly after the first
- lets the backfill design land on stable terminology from the start
- avoids having `job_run_key`, `run_key`, and `orchestrator_run_id` all enter
  the codebase at once and confuse scope boundaries

Do not wait until after orchestrator runs are implemented. That would make the
eventual rename more confusing and more expensive.

---

## Recommended Rename Set

Recommended final naming set:

- job field: `job_run_key`
- orchestrator field: `run_key`
- orchestrator row identity: `orchestrator_run_id`
- attempt identity: `dispatchio_attempt_id`
- helper: `resolve_job_run_key()`
- module: `run_resolution.py` or `run_keys.py`
- CLI job selector: `--job-run-key`
- CLI orchestrator selector: `--run-key`
- env var: `DISPATCHIO_JOB_RUN_KEY`

This is the clearest and most internally consistent naming scheme.

## Open Questions

None

## Closed questions

- Whether `run_reference` is still needed as a public term if `run_key` becomes
  the canonical orchestrator value.
  Answer: No - use `run_key`

- Whether user-facing job functions should continue to accept a positional
  `run_id` argument for ergonomics even if the internal contract becomes
  `job_run_key`
  Answer: No - hard cutover only

- Whether `run_id` should survive as a temporary compatibility alias only at the
  worker boundary
  Answer: No - hard cutover only

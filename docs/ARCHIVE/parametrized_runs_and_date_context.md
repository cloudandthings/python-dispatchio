# Parametrized Runs and DateContext

## Overview

This document specifies two related capabilities:

1. **DateContext** — a lazy date variable resolver injected into all executor
   template substitutions, replacing the need for users to pre-compute date
   strings outside dispatchio.

2. **Parametrized multi-run jobs** — a typed contract (`RunSpec`, `RunContext`)
   that allows a single job definition to produce one or more independently
   tracked submissions per tick, with explicit backfill-aware logic.

Together these are the foundation for jobs that have non-trivial date windowing
requirements, such as month-to-date queries with end-of-month overlap windows,
or queries that run differently during backfill than during normal scheduling.

This design builds on the existing backfill infrastructure described in
`backfill_and_replay.md`. The `is_backfill` flag on `RunContext` is derived from
the orchestrator run mode introduced there.

---

## New Types

### RunSpec

The typed return element from a `runs` callable. Separates run identity from
query parameters.

```python
@dataclass
class RunSpec:
    params: dict[str, str]       # Template vars merged with DateContext at submission
    variant: str | None = None   # Suffix appended to cadence-derived base run_key
    run_key: str | None = None   # Overrides the entire run_key; mutually exclusive with variant
```

`variant` and `run_key` are mutually exclusive. Setting both on the same `RunSpec`
is rejected at `Job.create()` time and again at tick time.

**`variant` validation:**

- Allowed characters: `[a-zA-Z0-9_-]`
- Max length: 64 characters
- `:` is explicitly rejected (reserved as the run_key separator)
- Duplicate variants within the same job and tick are rejected

Run key derivation:

| Fields set | Run key produced |
|---|---|
| neither | cadence-derived base key, e.g. `D20260428` |
| `variant="current_month"` | `D20260428:current_month` |
| `run_key="M202604"` | `M202604` (replaces base entirely) |

`run_key` is intended for cases where the desired run key cannot be expressed as
a suffix of the cadence-derived base — most commonly in backfill mode where a
daily job wants to collapse multiple daily reference times onto a single monthly
key so that state idempotency prevents redundant submissions. Use
`resolve_run_key(cadence, reference_time)` to produce standard-format keys rather
than building raw strings.

### RunContext

Passed into the `runs` callable on each tick evaluation.

```python
@dataclass
class RunContext:
    reference_time: datetime
    ctx: DateContext       # Lazy date variable resolver — see DateContext section
    is_backfill: bool
    job_name: str
```

`is_backfill` is `True` when the orchestrator run carrying this tick has
`mode=BACKFILL` (per `OrchestratorRunRecord.mode` from `backfill_and_replay.md`).

### DateContext

`DateContext` is a lazy resolver that computes date variable values on first
access. It implements `__getitem__` and `__missing__` only — it does not
pre-generate all combinations, and it supports `str.format_map()` directly
because Python's format machinery only requires `__getitem__`.

`__missing__` raises a clear `KeyError` naming the unrecognised variable rather
than silently returning an empty string.

`DateContext` is not a plain `dict`. A `dict()` conversion of a `DateContext`
instance returns only the variables that have been accessed so far, which is
useful for logging and debugging.

**On typed accessor alternatives:** builder patterns such as `day(0).yyyymmdd(rc)`
or nested attribute chains like `rc.ctx.day0.yyyymmdd` were considered. Neither
can be fully statically typed because the offset `N` is a runtime value.
The dict-style access `rc.ctx["day0_yyyymmdd"]` is explicit, consistent with how
the same variables appear in template strings, and requires no additional
machinery. Revisit if a language-server plugin is added.

---

## Date Variable Naming Convention

### Structure

```text
{period}{N}_{[anchor_]}{format}{[delimiter]}
```

| Component | Values | Notes |
|---|---|---|
| `period` | `day`, `wk`, `mon`, `qtr`, `yr` | Calendar period type |
| `N` | `0`, `1`, `2`, … | Periods-ago from `reference_time`. 0 = current period. |
| `anchor` | `first`, `last` | Selects the boundary day of a range period. Not used for `day`. Not used when accessing the period identifier itself (e.g. `mon0_yyyymm`). |
| `format` | See table below | Output format with no delimiters. |
| `delimiter` | Optional single character appended to format name | See delimiter table. |

### Format codes by period

| Period | Formats available | Example values |
|---|---|---|
| `day` | `yyyymmdd`, `yyyymm`, `yyyy`, `mm`, `dd` | `20260403`, `202604`, `2026`, `04`, `03` |
| `wk` | `yyyyww`, `ww`, `yyyy`; and `first`/`last` + `yyyymmdd` | `202614`, `14`, `2026`, `20260330`, `20260405` |
| `mon` | `yyyymm`, `yyyy`, `mm`; and `first`/`last` + `yyyymmdd` | `202604`, `2026`, `04`, `20260401`, `20260430` |
| `qtr` | `yyyyq`, `q`, `yyyy`; and `first`/`last` + `yyyymmdd` | `20261`, `1`, `2026`, `20260101`, `20260331` |
| `yr` | `yyyy`; and `first`/`last` + `yyyymmdd` | `2026`, `20260101`, `20261231` |

`yyyyq` produces a five-character string: four-digit year followed by the
quarter digit (e.g. `20261`, `20262`). This is unambiguous given the fixed
lengths of the other formats.

### Delimiter codes

The delimiter character is appended to the format name with no separator.

| Suffix | Character | `yyyymmdd` → | `yyyymm` → | `yyyyq` → |
|---|---|---|---|---|
| *(none)* | *(none)* | `20260401` | `202604` | `20261` |
| `D` | `-` dash | `2026-04-01` | `2026-04` | `2026-1` |
| `S` | ` ` space | `2026 04 01` | `2026 04` | `2026 1` |
| `L` | `/` slash | `2026/04/01` | `2026/04` | `2026/1` |

### Example variables

Reference time: `2026-04-03`

| Variable | Value | Notes |
|---|---|---|
| `day0_yyyymmdd` | `20260403` | Today |
| `day0_yyyymmddD` | `2026-04-03` | Today, dash-delimited |
| `day0_yyyymm` | `202604` | Month today falls in |
| `day0_dd` | `03` | Day component only |
| `day5_yyyymmdd` | `20260329` | Five days ago |
| `day5_yyyymm` | `202603` | Month five days ago falls in — crossed a boundary |
| `wk0_first_yyyymmdd` | `20260330` | First day of current week (Monday, ISO default) |
| `wk0_last_yyyymmdd` | `20260405` | Last day of current week |
| `wk0_yyyyww` | `202614` | ISO week identifier |
| `mon0_yyyymm` | `202604` | Current month identifier |
| `mon0_first_yyyymmdd` | `20260401` | First day of current month |
| `mon0_last_yyyymmdd` | `20260430` | Last day of current month |
| `mon0_first_yyyymmddD` | `2026-04-01` | First day of current month, dash-delimited |
| `mon1_yyyymm` | `202603` | Previous month identifier |
| `mon1_first_yyyymmdd` | `20260301` | First day of previous month |
| `mon1_last_yyyymmdd` | `20260331` | Last day of previous month |
| `qtr0_yyyyq` | `20261` | Current quarter identifier |
| `qtr0_q` | `1` | Current quarter number |
| `qtr0_first_yyyymmdd` | `20260101` | First day of current quarter |
| `qtr0_last_yyyymmdd` | `20260331` | Last day of current quarter |
| `qtr1_first_yyyymmdd` | `20251001` | First day of previous quarter |
| `yr0_yyyy` | `2026` | Current year |
| `yr0_first_yyyymmdd` | `20260101` | First day of current year |
| `yr0_last_yyyymmdd` | `20261231` | Last day of current year |

### Supported offset range

N may be any integer from 0 to 365 inclusive. Values outside this range are
rejected by `__missing__` with a clear error. There is no practical reason to
look back more than a year via the offset mechanism; larger lookbacks should be
expressed as explicit date strings in `RunSpec.params`.

### Template substitution

`DateContext` variables are injected into template substitution alongside the
existing `{run_key}`, `{job_name}`, and `{reference_time}` variables. They are
available in all executor types: `SubprocessJob`, `PythonJob`, `HttpJob`,
`AthenaJob`, `LambdaJob`, and `StepFunctionJob`.

`RunSpec.params` values are merged into the substitution context after
`DateContext` and take priority over it. An explicit param will override a
derived date variable if the keys collide.

---

## New Cadences

The `Cadence` enum gains two entries:

```python
QUARTERLY  # one run per calendar quarter; respects quarter_start_month setting
ANNUALLY   # one run per calendar year; respects quarter_start_month for year boundaries
```

These are valid values for `cadence` on `Job` and for `resolve_run_key()`.

Quarterly run keys follow the pattern `Q{yyyyq}`, e.g. `Q20261`. Annual run keys
follow `Y{yyyy}`, e.g. `Y2026`.

---

## Global Date Settings

A new `[dispatchio.dates]` section in `dispatchio.toml` controls date
conventions used by `DateContext` and the new cadences.

```toml
[dispatchio.dates]
week_start_day = 0        # 0=Monday (ISO default), 6=Sunday. Affects wk period and WEEKLY cadence.
quarter_start_month = 1   # 1=January (default), 4=April (UK fiscal), 7=July, 10=October.
                          # Affects qtr period, QUARTERLY cadence, and yr period boundaries
                          # when a fiscal year is in use.
```

Both settings default to their ISO/Gregorian standard values if omitted.

---

## Job Model Changes

```python
Job.create(
    name: str,
    executor: ExecutorConfig,
    cadence: Cadence | None = None,
    runs: Callable[[RunContext], list[RunSpec]] | None = None,   # NEW
    # ... all existing parameters unchanged
)
```

### `runs`

`None` is equivalent to `lambda rc: [RunSpec(params={})]` — a single implicit
run with no variant suffix, preserving full backward compatibility.

When a job needs to run at a coarser granularity during backfill (e.g. once per
month instead of once per day), the `runs` callable handles this by returning a
`RunSpec` with `run_key=resolve_run_key(MONTHLY, rc.reference_time)`. Because
`backfill-enqueue` iterates through every day in the requested range and calls
`runs()` for each, multiple daily reference times produce the same monthly
run_key. State idempotency skips all but the first — no special orchestrator
logic or `backfill_cadence` parameter is required.

---

## Orchestrator Changes

Phase 3 (job evaluation) is extended to expand each job into its `RunSpec` list
before performing state lookups.

For each job at each tick:

1. Call `runs(RunContext(reference_time, ctx, is_backfill, job_name))` →
   `list[RunSpec]`
2. Validate the list: if more than one `RunSpec` is returned, all must have
   non-`None` `variant` or `run_key`, and the resolved run_keys must be unique
   within the list.
3. For each `RunSpec`: resolve the run_key —
   - `run_key` set: use it directly
   - `variant` set: `{cadence_base_key}:{variant}`
   - neither: `{cadence_base_key}` (single-run backward compat)
4. Deduplicate: if two `RunSpec`s in the list resolve to the same run_key,
   raise a validation error naming the job and the conflicting key.
5. Evaluate each `(job, run_key)` pair independently against state (skip if
   DONE, submit if PENDING, apply retry logic if ERROR/LOST, etc.)
6. Build the template context for submission as `DateContext` variables merged
   with `RunSpec.params`

Validation errors in step 2 are raised at tick time with a message that
identifies the job name and the specific problem. They do not abort the entire
tick — other jobs continue to be evaluated.

---

## State Model Changes

Run keys are stored as plain strings. No schema change is required.

The `:` character is used as the separator when `variant` is set
(`D20260428:current_month`). It does not appear in any existing cadence-derived
key format, so it is safe as a delimiter. `run_key` overrides (e.g. `M202604`)
follow standard cadence key formats and require no separator.

`dispatchio status --job <name> --run-key D20260428` returns all records whose
`run_key` starts with `D20260428`, covering both the base key and any variants.

---

## CLI Changes

### `backfill-plan` (extended)

The existing command gains an optional `--order` flag and now surfaces the
status of existing runs alongside new ones.

```bash
dispatchio backfill-plan \
    --start 2025-01-01 \
    --end 2025-12-31 \
    [--order asc|desc]          # optional; derived from --start/--end if omitted
    [--job <name>]              # omit to plan all jobs in the orchestrator
```

Order derivation: if `--start` is earlier than `--end` the default is `asc`;
if `--start` is later than `--end` the default is `desc`. Pass `--order`
explicitly to override this, for example if the dates were supplied in an
unexpected order.

When a job's `runs` callable uses `run_key` overrides, the plan deduplicates
across the date range and shows only unique keys:

```text
M202501   -       (new)
M202502   DONE    (already run; pass --overwrite to re-enqueue)
M202503   -       (new)
...
11 new, 1 already run
```

`backfill-plan` never mutates state. It is the recommended first step before
any `backfill-enqueue`.

### `backfill-enqueue` (extended)

Gains an optional `--order` flag (same derivation rules as `backfill-plan`) and
an `--overwrite` flag for runs that have already been submitted.

```bash
dispatchio backfill-enqueue \
    --start 2025-01-01 \
    --end 2025-12-31 \
    [--order asc|desc] \        # optional; derived from --start/--end if omitted
    [--job <name>] \
    [--submitted-by <name>] \
    [--reason <text>] \
    [--overwrite]               # re-enqueue runs that are already in state
```

Without `--overwrite`: runs that already exist in state are skipped with a
notice. This is the expected behaviour during incremental backfill — many runs
will already be complete and skipping them is correct. Summary: "Enqueued 11,
skipped 1 already run. Pass --overwrite to re-enqueue."

With `--overwrite`: already-present runs are reset to PENDING and enqueued
alongside new ones. Use this to reprocess a period that needs to be re-run.

### `dispatch` (new)

Manually submits one run with explicit params, bypassing the `runs` callable
entirely. Intended for ad-hoc corrections, targeted backfills of a single
period, and testing.

```bash
dispatchio dispatch \
    --job <name> \
    (--variant <variant> | --run-key <key>)   # one required; mutually exclusive
    [--param key=value ...] \   # merged into template context as RunSpec.params
    [--run-date YYYY-MM-DD] \   # sets reference_time for DateContext; defaults to today
    [--submitted-by <name>] \
    [--reason <text>] \
    [--dry-run] \               # print what would be enqueued without touching state
    [--overwrite]               # re-enqueue if this run_key already exists in state
```

`--variant` appends to the cadence-derived base key for the given `--run-date`.
`--run-key` sets the entire run_key explicitly, independent of cadence.

`--dry-run` prints the resolved run_key, the merged template context, and
whether the run_key already exists in state — without writing anything.

`--run-date` is distinct from `tick --reference-time`. It controls the
`DateContext` base date for param derivation only; the explicit `--param` values
take priority over anything DateContext would derive.

`dispatch` is distinct from `replay-enqueue`. `replay-enqueue` re-runs existing
run_keys. `dispatch` creates a new run_key (potentially one that has never
existed) with caller-supplied params.

---

## Working Example

See `examples/parametrized_runs/jobs.py` for a complete job definition using
`RunContext`, `RunSpec`, and `DateContext` variables.

The example models a daily AWS Athena query with:

- month-to-date date range in normal operation
- a five-day overlap window that adds a full previous-month query
- full-month date range during backfill, with `run_key` collapsing 365 daily
  reference times to 12 unique monthly keys via state idempotency

```bash
# Plan a full-year backfill before committing
dispatchio backfill-plan \
    --job athena_daily_events \
    --start 2025-01-01 --end 2025-12-31 \
    --order asc

# Enqueue — 12 monthly runs, skips any already done
dispatchio backfill-enqueue \
    --job athena_daily_events \
    --start 2025-01-01 --end 2025-12-31 \
    --order asc --submitted-by bjorn --reason "initial backfill"

# One-off correction with explicit dates
dispatchio dispatch \
    --job athena_daily_events \
    --variant q1_correction \
    --param start_date=2025-01-01 \
    --param end_date=2025-03-31 \
    --run-date 2025-03-31 \
    --submitted-by bjorn \
    --reason "restate Q1 after source fix"
```

---

## Delivery Phases

### Phase 1 — DateContext and template substitution

- Implement `DateContext` lazy resolver with `__getitem__` and `__missing__`
- Add `week_start_day` and `quarter_start_month` to global settings
- Inject `DateContext` into template substitution for all executor types
- Add `QUARTERLY` and `ANNUALLY` cadences
- No changes to `Job` model or orchestrator logic

### Phase 2 — RunSpec, RunContext, and multi-run job support

- Implement `RunSpec` and `RunContext` dataclasses with validation
- Add `runs` parameter to `Job.create()`
- Extend Phase 3 evaluation to expand jobs into `RunSpec` lists, resolving
  `run_key` overrides and deduplicating within a tick
- Update state queries to support variant suffix matching

### Phase 3 — Backfill integration

- Pass `is_backfill` from orchestrator run mode into `RunContext`
- Extend `backfill-plan` with optional `--order`, `--job`, deduplication of
  `run_key` overrides across the date range, and per-run-key status display
- Extend `backfill-enqueue` with optional `--order` and `--overwrite` flag
- Implement `dispatch` command with `--variant` / `--run-key` / `--dry-run` flags

---

## Implementation Notes

- `DateContext.__getitem__` should parse the variable name with a single regex
  rather than string splitting, to keep the parser unambiguous and easy to test.
- The `:` character in run_keys is safe as a separator because existing key
  formats use only `[DMWHQY]` prefix + digits. Add an assertion to the run_key
  generation path to catch regressions if new formats are introduced.
- `variant` and `run_key` validation should be a shared utility called from
  `Job.create()`, the tick evaluator, and the `dispatch` command.
- `QUARTERLY` and `ANNUALLY` run key formats (`Q{yyyyq}`, `Y{yyyy}`) must be
  registered in `resolve_run_key()` alongside the existing daily/monthly/weekly
  formats.
- `backfill-plan` must call `runs()` for every reference time in the requested
  range and deduplicate the resulting run_keys before displaying the plan. This
  ensures that `run_key` overrides (e.g. monthly keys from a daily job) are
  shown correctly rather than once per day.

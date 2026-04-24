# Job and Namespace Rename Support

## Problem

Jobs and namespaces are identified by string names denormalized across all history tables (`attempts`, `dead_letters`, `orchestrator_runs`, `event_occurrences`). Renaming a job or namespace today orphans all historical records and breaks downstream dependency references.

## Approach

Add stable UUID surrogate keys for jobs and namespaces. History tables reference the UUID, so the display name becomes mutable. Users never see or declare UUIDs — they're generated and resolved automatically.

## New Registry Tables

```
namespaces
  namespace_id  UUID  PK
  name          TEXT  UNIQUE NOT NULL
  created_at    DATETIME

jobs
  job_id        UUID  PK
  namespace_id  UUID  FK → namespaces
  name          TEXT  NOT NULL
  created_at    DATETIME
  UNIQUE(namespace_id, name)

events
  event_id      UUID  PK
  namespace_id  UUID  FK → namespaces
  name          TEXT  NOT NULL
  created_at    DATETIME
  UNIQUE(namespace_id, name)
```

## Schema Changes

| Table | Remove | Add |
|---|---|---|
| `attempts` | `namespace TEXT`, `job_name TEXT` | `job_id UUID FK → jobs` |
| `orchestrator_runs` | `namespace TEXT` | `namespace_id UUID FK → namespaces` |
| `event_occurrences` | `namespace TEXT`, `name TEXT` (part of PK) | `event_id UUID FK → events` |
| `dead_letters` | `namespace TEXT`, `job_name TEXT` | `namespace_id UUID NULLABLE`, `job_id UUID NULLABLE` |
| `retry_requests` | — | no change (see note below) |

**`dead_letters`** has minimal integrity constraints — all fields are best-effort. `namespace_id` and `job_id` are nullable with no FK enforcement; they are populated opportunistically at insert time if the incoming event can be correlated. No `raw_job_name` field is added.

**`retry_requests`** stores job names in JSON columns (`requested_jobs`, `selected_jobs`, `assigned_attempt_by_job`). These are audit snapshots — they intentionally preserve the names as they were at request time and are not updated on rename.

## Registration Behaviour

On orchestrator startup:

1. Look up the orchestrator `namespace` name in `namespaces`; insert if absent → `namespace_id`.
2. For each job: look up `(namespace_id, job_name)` in `jobs`; insert if absent → `job_id`.
3. For each `EventDependency`: look up `(namespace_id, event_name)` in `events`; insert if absent → `event_id`.
4. When an event is written via `set_event`, look up or create the entry in `events` the same way.

This is the only place UUIDs are created. The user's code is unchanged.

## Rename Workflow

**Step 1 — run the CLI command before changing code:**

```
dispatchio job rename <old_name> <new_name> [--namespace <ns>]
dispatchio namespace rename <old_name> <new_name>
dispatchio event rename <old_name> <new_name> [--namespace <ns>]
```

Both are a single `UPDATE … SET name = …` on the registry table. All history rows reference the UUID and automatically resolve to the new name.

**Step 2 — rename in code.** On next startup the job/namespace registers under the new name and resolves to the same existing UUID.

The CLI command must run first. If code is renamed first, the orchestrator mints a new UUID for the new name and the old records become orphaned.

## Recovery: `remap` Command

If code was renamed before the CLI command was run:

```
dispatchio job remap <old_name> <new_name> [--namespace <ns>]
dispatchio namespace remap <old_name> <new_name>
dispatchio event remap <old_name> <new_name> [--namespace <ns>]
```

Same underlying operation as `rename`, but targets an orphaned entry — one that exists in the registry but has no live registration. The command validates that `old_name` is not currently registered before applying the update.

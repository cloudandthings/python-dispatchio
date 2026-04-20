"""Admission-control engine for per-tick submission decisions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from dispatchio.models import AdmissionPolicy, AttemptRecord, JobAction, Status


@dataclass(frozen=True)
class AdmissionCandidate:
    """Single pending submission candidate considered for admission."""

    index: int
    job_name: str
    pool: str
    priority: int
    definition_order: int


@dataclass(frozen=True)
class DeferredDecision:
    """Deferred candidate metadata produced by admission checks."""

    action: JobAction
    detail: str


@dataclass(frozen=True)
class AdmissionPlan:
    """Admission outcome for a single tick."""

    admitted_indices: list[int]
    deferred_by_index: dict[int, DeferredDecision]


def build_admission_plan(
    *,
    candidates: list[AdmissionCandidate],
    active_attempts: list[AttemptRecord],
    job_pool_by_name: dict[str, str],
    policy: AdmissionPolicy,
) -> AdmissionPlan:
    """Admit candidates according to global/pool active and per-tick submit limits."""

    active_jobs = 0
    active_jobs_by_pool: dict[str, int] = defaultdict(int)
    active_statuses = Status.active()

    for attempt in active_attempts:
        if attempt.status not in active_statuses:
            continue
        active_jobs += 1
        pool = job_pool_by_name.get(attempt.job_name)
        if pool is not None:
            active_jobs_by_pool[pool] += 1

    submit_jobs_this_tick = 0
    submit_jobs_this_tick_by_pool: dict[str, int] = defaultdict(int)

    admitted_indices: list[int] = []
    deferred_by_index: dict[int, DeferredDecision] = {}

    for candidate in sorted(
        candidates,
        key=lambda c: (-c.priority, c.definition_order),
    ):
        pool_policy = policy.pools.get(candidate.pool)

        global_active_cap = policy.max_active_jobs
        if global_active_cap is not None and active_jobs >= global_active_cap:
            deferred_by_index[candidate.index] = DeferredDecision(
                action=JobAction.DEFERRED_ACTIVE_LIMIT,
                detail=f"active={active_jobs}/{global_active_cap}",
            )
            continue

        pool_active_cap = (
            pool_policy.max_active_jobs if pool_policy is not None else None
        )
        pool_active_now = active_jobs_by_pool[candidate.pool]
        if pool_active_cap is not None and pool_active_now >= pool_active_cap:
            deferred_by_index[candidate.index] = DeferredDecision(
                action=JobAction.DEFERRED_POOL_ACTIVE_LIMIT,
                detail=f"pool={candidate.pool} active={pool_active_now}/{pool_active_cap}",
            )
            continue

        global_submit_cap = policy.max_submit_jobs_per_tick
        if global_submit_cap is not None and submit_jobs_this_tick >= global_submit_cap:
            deferred_by_index[candidate.index] = DeferredDecision(
                action=JobAction.DEFERRED_SUBMIT_LIMIT,
                detail=(
                    f"submit_jobs_this_tick={submit_jobs_this_tick}/{global_submit_cap}"
                ),
            )
            continue

        pool_submit_cap = (
            pool_policy.max_submit_jobs_per_tick if pool_policy is not None else None
        )
        pool_submit_now = submit_jobs_this_tick_by_pool[candidate.pool]
        if pool_submit_cap is not None and pool_submit_now >= pool_submit_cap:
            deferred_by_index[candidate.index] = DeferredDecision(
                action=JobAction.DEFERRED_POOL_SUBMIT_LIMIT,
                detail=(
                    f"pool={candidate.pool} "
                    f"submit_jobs_this_tick={pool_submit_now}/{pool_submit_cap}"
                ),
            )
            continue

        admitted_indices.append(candidate.index)
        active_jobs += 1
        active_jobs_by_pool[candidate.pool] += 1
        submit_jobs_this_tick += 1
        submit_jobs_this_tick_by_pool[candidate.pool] += 1

    return AdmissionPlan(
        admitted_indices=admitted_indices,
        deferred_by_index=deferred_by_index,
    )

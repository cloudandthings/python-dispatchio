"""Tests for the Event dependency system.

Covers:
  - event_dependency() helper
  - Event model and StateStore event API
  - Orchestrator satisfying EventDependency from event store
  - EventDependency validation in the job graph
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dispatchio import (
    DAILY,
    Job,
    SubprocessJob,
)
from dispatchio.cadence import DateCadence, Frequency
from dispatchio.events import event_dependency
from dispatchio.executor.base import BaseExecutor
from dispatchio.models import (
    Event,
    EventDependency,
    JobAction,
    JobDependency,
    Status,
)
from dispatchio.orchestrator import Orchestrator
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore

REF = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
RUN_KEY = "D20250115"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _store() -> SQLAlchemyStateStore:
    return SQLAlchemyStateStore("sqlite:///:memory:")


def _job(name: str, **kwargs) -> Job:
    return Job(
        name=name,
        executor=SubprocessJob(command=["echo", name]),
        cadence=DAILY,
        **kwargs,
    )


class SpyExecutor(BaseExecutor):
    def __init__(self):
        self.calls: list[str] = []

    def submit(self, job, attempt, reference_time, timeout=None):
        self.calls.append(job.name)


def _make_orch(jobs, store=None, **kwargs):
    store = store or _store()
    executor = SpyExecutor()
    orch = Orchestrator(
        jobs=jobs,
        state=store,
        executors={"subprocess": executor},
        **kwargs,
    )
    return orch, store, executor


# ---------------------------------------------------------------------------
# event_dependency() helper
# ---------------------------------------------------------------------------


class TestEventDependencyHelper:
    def test_returns_event_dependency(self):
        dep = event_dependency("user_registered")
        assert isinstance(dep, EventDependency)

    def test_event_name_stored(self):
        dep = event_dependency("kyc_passed")
        assert dep.event_name == "kyc_passed"

    def test_default_required_status_is_done(self):
        dep = event_dependency("user_registered")
        assert dep.required_status == Status.DONE

    def test_custom_required_status(self):
        dep = event_dependency("data_ready", required_status=Status.ERROR)
        assert dep.required_status == Status.ERROR

    def test_cadence_defaults_to_none(self):
        dep = event_dependency("user_registered")
        assert dep.cadence is None

    def test_cadence_stored(self):
        dep = event_dependency("user_registered", cadence=DAILY)
        assert dep.cadence == DAILY

    def test_kind_discriminator_is_event(self):
        dep = event_dependency("something")
        assert dep.kind == "event"

    def test_no_prefix_required(self):
        dep = event_dependency("plain_name")
        assert dep.event_name == "plain_name"


# ---------------------------------------------------------------------------
# Event model and StateStore API
# ---------------------------------------------------------------------------


class TestEventStoreAPI:
    def test_set_and_get_event(self):
        store = _store()
        event = Event(name="user_registered", run_key=RUN_KEY)
        store.set_event(event)
        result = store.get_event("user_registered", RUN_KEY)
        assert result is not None
        assert result.name == "user_registered"
        assert result.run_key == RUN_KEY
        assert result.status == Status.DONE

    def test_get_event_returns_none_when_missing(self):
        store = _store()
        assert store.get_event("nonexistent", RUN_KEY) is None

    def test_set_event_is_upsert(self):
        store = _store()
        store.set_event(Event(name="data_ready", run_key=RUN_KEY, status=Status.DONE))
        store.set_event(Event(name="data_ready", run_key=RUN_KEY, status=Status.ERROR))
        result = store.get_event("data_ready", RUN_KEY)
        assert result is not None
        assert result.status == Status.ERROR

    def test_events_are_keyed_by_name_and_run_key(self):
        store = _store()
        store.set_event(Event(name="ev", run_key="D20250115"))
        store.set_event(Event(name="ev", run_key="D20250116"))
        assert store.get_event("ev", "D20250115") is not None
        assert store.get_event("ev", "D20250116") is not None
        assert store.get_event("ev", "D20250117") is None

    def test_list_events_returns_all(self):
        store = _store()
        store.set_event(Event(name="a", run_key=RUN_KEY))
        store.set_event(Event(name="b", run_key=RUN_KEY))
        events = store.list_events()
        names = {e.name for e in events}
        assert names == {"a", "b"}

    def test_list_events_filtered_by_run_key(self):
        store = _store()
        store.set_event(Event(name="ev", run_key="D20250115"))
        store.set_event(Event(name="ev", run_key="D20250116"))
        results = store.list_events(run_key="D20250115")
        assert len(results) == 1
        assert results[0].run_key == "D20250115"

    def test_event_status_non_done(self):
        store = _store()
        store.set_event(
            Event(name="failing_feed", run_key=RUN_KEY, status=Status.ERROR)
        )
        result = store.get_event("failing_feed", RUN_KEY)
        assert result is not None
        assert result.status == Status.ERROR


# ---------------------------------------------------------------------------
# Orchestrator satisfies EventDependency
# ---------------------------------------------------------------------------


class TestOrchestratorEventDependency:
    def test_job_blocked_until_event_emitted(self):
        store = _store()
        consumer = _job(
            "consumer",
            depends_on=[event_dependency("user_registered", cadence=DAILY)],
        )
        orch, store, executor = _make_orch(
            [consumer], store=store, strict_dependencies=False
        )

        result = orch.tick(REF)
        assert all(r.action != JobAction.SUBMITTED for r in result.results)
        assert not executor.calls

    def test_job_unblocked_after_event_emitted(self):
        store = _store()
        consumer = _job(
            "consumer",
            depends_on=[event_dependency("user_registered", cadence=DAILY)],
        )
        orch, store, executor = _make_orch(
            [consumer], store=store, strict_dependencies=False
        )

        store.set_event(Event(name="user_registered", run_key=RUN_KEY))
        result = orch.tick(REF)
        assert any(
            r.job_name == "consumer" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_fan_in_requires_all_events(self):
        store = _store()
        consumer = _job(
            "consumer",
            depends_on=[
                event_dependency("user_registered", cadence=DAILY),
                event_dependency("kyc_passed", cadence=DAILY),
            ],
        )
        orch, store, executor = _make_orch(
            [consumer], store=store, strict_dependencies=False
        )

        # Only first event — still blocked
        store.set_event(Event(name="user_registered", run_key=RUN_KEY))
        result = orch.tick(REF)
        assert all(r.action != JobAction.SUBMITTED for r in result.results)

        # Both events — now unblocked
        store.set_event(Event(name="kyc_passed", run_key=RUN_KEY))
        result = orch.tick(REF)
        assert any(
            r.job_name == "consumer" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_event_and_job_dependency_both_required(self):
        store = _store()
        upstream = _job("upstream")
        consumer = _job(
            "consumer",
            depends_on=[
                JobDependency(job_name="upstream", cadence=DAILY),
                event_dependency("external_signal", cadence=DAILY),
            ],
        )
        orch, store, executor = _make_orch(
            [upstream, consumer], store=store, strict_dependencies=False
        )

        # Tick 1: upstream submits, consumer blocked on both
        orch.tick(REF)
        attempt = store.get_latest_attempt("upstream", RUN_KEY)
        store.update_attempt(attempt.model_copy(update={"status": Status.DONE}))

        # upstream done, but event still missing — consumer still blocked
        result = orch.tick(REF)
        assert all(
            r.job_name != "consumer" or r.action != JobAction.SUBMITTED
            for r in result.results
        )

        # Both satisfied
        store.set_event(Event(name="external_signal", run_key=RUN_KEY))
        result = orch.tick(REF)
        assert any(
            r.job_name == "consumer" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_event_run_key_matches_cadence_of_consumer(self):
        """Event for yesterday's run_key does not unblock a job expecting today's."""
        store = _store()
        consumer = _job(
            "consumer",
            depends_on=[event_dependency("daily_feed", cadence=DAILY)],
        )
        orch, store, executor = _make_orch(
            [consumer], store=store, strict_dependencies=False
        )

        # Write event for yesterday's key — should NOT satisfy today's dep
        store.set_event(Event(name="daily_feed", run_key="D20250114"))
        result = orch.tick(REF)
        assert all(r.action != JobAction.SUBMITTED for r in result.results)

    def test_required_status_error_satisfied_by_error_event(self):
        store = _store()
        consumer = _job(
            "consumer",
            depends_on=[
                event_dependency("feed", cadence=DAILY, required_status=Status.ERROR)
            ],
        )
        orch, store, executor = _make_orch(
            [consumer], store=store, strict_dependencies=False
        )

        store.set_event(Event(name="feed", run_key=RUN_KEY, status=Status.ERROR))
        result = orch.tick(REF)
        assert any(
            r.job_name == "consumer" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_required_status_mismatch_keeps_job_blocked(self):
        store = _store()
        consumer = _job(
            "consumer",
            depends_on=[
                event_dependency("feed", cadence=DAILY, required_status=Status.DONE)
            ],
        )
        orch, store, executor = _make_orch(
            [consumer], store=store, strict_dependencies=False
        )

        # Event is ERROR but consumer requires DONE
        store.set_event(Event(name="feed", run_key=RUN_KEY, status=Status.ERROR))
        result = orch.tick(REF)
        assert all(r.action != JobAction.SUBMITTED for r in result.results)

    def test_event_dep_with_offset_cadence(self):
        """EventDependency with yesterday's cadence checks yesterday's run_key."""
        store = _store()
        yesterday = DateCadence(frequency=Frequency.DAILY, offset=-1)
        consumer = _job(
            "consumer",
            depends_on=[event_dependency("daily_feed", cadence=yesterday)],
        )
        orch, store, executor = _make_orch(
            [consumer], store=store, strict_dependencies=False
        )

        store.set_event(Event(name="daily_feed", run_key="D20250114"))
        result = orch.tick(REF)
        assert any(
            r.job_name == "consumer" and r.action == JobAction.SUBMITTED
            for r in result.results
        )


# ---------------------------------------------------------------------------
# Job graph validation with EventDependency
# ---------------------------------------------------------------------------


class TestEventDependencyInGraph:
    def test_event_dep_not_validated_against_job_index(self):
        """EventDependency names are never in the job index — no strict error."""
        consumer = _job(
            "consumer",
            depends_on=[event_dependency("some_signal", cadence=DAILY)],
        )
        # strict_dependencies=True (default) should not raise for EventDependency
        orch, _, _ = _make_orch([consumer])
        assert orch is not None

    def test_job_dep_for_unknown_name_raises_with_strict(self):
        consumer = _job(
            "consumer",
            depends_on=[JobDependency(job_name="nonexistent", cadence=DAILY)],
        )
        with pytest.raises(ValueError, match="Unresolved dependencies"):
            _make_orch([consumer])

    def test_event_dep_and_strict_true_coexist(self):
        """strict_dependencies=True still enforces job deps while ignoring event deps."""
        upstream = _job("upstream")
        consumer = _job(
            "consumer",
            depends_on=[
                JobDependency(job_name="upstream", cadence=DAILY),
                event_dependency("external_signal", cadence=DAILY),
            ],
        )
        orch, _, _ = _make_orch([upstream, consumer])
        assert orch is not None


# ---------------------------------------------------------------------------
# Cross-namespace event targeting
# ---------------------------------------------------------------------------


def _shared_stores(ns_a: str, ns_b: str):
    """Two scoped stores and one unscoped store backed by the same SQLite DB."""
    store_a = SQLAlchemyStateStore("sqlite:///:memory:", namespace=ns_a)

    def _clone(ns):
        s = SQLAlchemyStateStore.__new__(SQLAlchemyStateStore)
        s._engine = store_a._engine
        s._Session = store_a._Session
        s._lock = store_a._lock
        s._namespace = ns
        return s

    return store_a, _clone(ns_b), _clone(None)


class TestCrossNamespaceEvents:
    def test_event_addressed_to_other_namespace_not_visible_here(self):
        """An event targeting orch_b must not appear in orch_a's get_event."""
        store_a, store_b, _ = _shared_stores("orch_a", "orch_b")
        store_a.set_event(Event(namespace="orch_b", name="signal", run_key=RUN_KEY))

        assert store_a.get_event("signal", RUN_KEY) is None

    def test_event_addressed_to_namespace_is_visible_there(self):
        """An event targeting orch_b is visible to a store scoped to orch_b."""
        store_a, store_b, _ = _shared_stores("orch_a", "orch_b")
        store_a.set_event(Event(namespace="orch_b", name="signal", run_key=RUN_KEY))

        result = store_b.get_event("signal", RUN_KEY)
        assert result is not None
        assert result.namespace == "orch_b"
        assert result.name == "signal"

    def test_same_event_name_distinct_per_namespace(self):
        """(namespace, name, run_key) are independent — no collision across namespaces."""
        store_a, store_b, _ = _shared_stores("orch_a", "orch_b")
        store_a.set_event(
            Event(namespace="orch_a", name="feed", run_key=RUN_KEY, status=Status.DONE)
        )
        store_a.set_event(
            Event(namespace="orch_b", name="feed", run_key=RUN_KEY, status=Status.ERROR)
        )

        assert store_a.get_event("feed", RUN_KEY).status == Status.DONE
        assert store_b.get_event("feed", RUN_KEY).status == Status.ERROR

    def test_list_events_scoped_store_only_sees_own_namespace(self):
        store_a, store_b, _ = _shared_stores("orch_a", "orch_b")
        store_a.set_event(Event(namespace="orch_a", name="ev", run_key=RUN_KEY))
        store_a.set_event(Event(namespace="orch_b", name="ev", run_key=RUN_KEY))

        events_a = store_a.list_events()
        assert all(e.namespace == "orch_a" for e in events_a)
        assert len(events_a) == 1

    def test_list_events_unscoped_store_sees_all_namespaces(self):
        store_a, store_b, unscoped = _shared_stores("orch_a", "orch_b")
        store_a.set_event(Event(namespace="orch_a", name="ev", run_key=RUN_KEY))
        store_a.set_event(Event(namespace="orch_b", name="ev", run_key=RUN_KEY))

        events = unscoped.list_events()
        namespaces = {e.namespace for e in events}
        assert namespaces == {"orch_a", "orch_b"}

    def test_get_event_raises_on_unscoped_store(self):
        from dispatchio.state.base import AmbiguousNamespaceError

        _, _, unscoped = _shared_stores("orch_a", "orch_b")
        with pytest.raises(AmbiguousNamespaceError):
            unscoped.get_event("signal", RUN_KEY)

    def test_cross_namespace_event_satisfies_orchestrator_dependency(self):
        """Orchestrator 'orch_b' picks up an event emitted targeting its namespace."""
        store_a, store_b, _ = _shared_stores("orch_a", "orch_b")

        consumer = _job(
            "consumer",
            depends_on=[event_dependency("upstream_ready", cadence=DAILY)],
        )
        orch_b = Orchestrator(
            jobs=[consumer],
            state=store_b,
            executors={"subprocess": SpyExecutor()},
            strict_dependencies=False,
        )

        # Before signal: blocked
        result = orch_b.tick(REF)
        assert all(r.action != JobAction.SUBMITTED for r in result.results)

        # orch_a emits event targeting orch_b's namespace
        store_a.set_event(
            Event(namespace="orch_b", name="upstream_ready", run_key=RUN_KEY)
        )

        # After signal: unblocked
        result = orch_b.tick(REF)
        assert any(
            r.job_name == "consumer" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

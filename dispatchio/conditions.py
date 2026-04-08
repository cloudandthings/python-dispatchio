"""
Condition — polymorphic schedule gate for Job.

A Condition decides whether a job is eligible to run on a given tick, based
on the tick's reference_time and the job's cadence.

Public API (re-exported from dispatchio):
    Condition             — Protocol; implement is_met() for custom conditions
    TimeOfDayCondition    — gate on wall-clock time       (after / before)
    MinuteOfHourCondition — gate on minute within hour    (after / before)
    DayOfMonthCondition   — gate on calendar day in month (after / before)
    DayOfWeekCondition    — gate on day of week           (on_days)
    AllOf                 — AND composite
    AnyOf                 — OR  composite
"""

from __future__ import annotations

from datetime import datetime, time
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import BaseModel, Field, model_validator

if TYPE_CHECKING:
    from dispatchio.cadence import Cadence


# ---------------------------------------------------------------------------
# Protocol (for structural type checking — custom condition implementations)
# ---------------------------------------------------------------------------

from typing import Protocol, runtime_checkable


@runtime_checkable
class Condition(Protocol):
    """
    Structural protocol satisfied by any object implementing is_met().

    The built-in condition types (TimeOfDayCondition, DayOfMonthCondition,
    etc.) satisfy this protocol automatically. Implement it for custom gates.
    """
    def is_met(self, reference_time: datetime, cadence: "Cadence") -> bool: ...


# ---------------------------------------------------------------------------
# Concrete condition types
# ---------------------------------------------------------------------------

class TimeOfDayCondition(BaseModel):
    """Gate on wall-clock time. Natural fit for daily / weekly / monthly jobs."""
    type:   Literal["time_of_day"] = "time_of_day"
    after:  time | None = None    # ref.time() >= after
    before: time | None = None    # ref.time() <  before

    @model_validator(mode="after")
    def _at_least_one(self) -> TimeOfDayCondition:
        if self.after is None and self.before is None:
            raise ValueError("at least one of 'after' or 'before' must be set")
        return self

    def is_met(self, reference_time: datetime, cadence: "Cadence") -> bool:
        ref_t = reference_time.time()   # tzinfo stripped; always naive
        if self.after is not None and ref_t < self.after:
            return False
        if self.before is not None and ref_t >= self.before:
            return False
        return True


class MinuteOfHourCondition(BaseModel):
    """Gate on minute within the current hour. Natural fit for hourly jobs."""
    type:   Literal["minute_of_hour"] = "minute_of_hour"
    after:  int | None = None    # ref.minute >= after  (0–59)
    before: int | None = None    # ref.minute <  before (0–59)

    @model_validator(mode="after")
    def _at_least_one(self) -> MinuteOfHourCondition:
        if self.after is None and self.before is None:
            raise ValueError("at least one of 'after' or 'before' must be set")
        return self

    def is_met(self, reference_time: datetime, cadence: "Cadence") -> bool:
        m = reference_time.minute
        if self.after is not None and m < self.after:
            return False
        if self.before is not None and m >= self.before:
            return False
        return True


class DayOfMonthCondition(BaseModel):
    """Gate on calendar day within the current month. Natural fit for monthly jobs."""
    type:   Literal["day_of_month"] = "day_of_month"
    after:  int | None = None    # ref.day >= after   (1–31)
    before: int | None = None    # ref.day <  before  (1–31)

    @model_validator(mode="after")
    def _at_least_one(self) -> DayOfMonthCondition:
        if self.after is None and self.before is None:
            raise ValueError("at least one of 'after' or 'before' must be set")
        return self

    def is_met(self, reference_time: datetime, cadence: "Cadence") -> bool:
        d = reference_time.day
        if self.after is not None and d < self.after:
            return False
        if self.before is not None and d >= self.before:
            return False
        return True


class DayOfWeekCondition(BaseModel):
    """Gate on day of week. Applies across any cadence."""
    type:    Literal["day_of_week"] = "day_of_week"
    on_days: list[int]              # 0 = Mon … 6 = Sun

    def is_met(self, reference_time: datetime, cadence: "Cadence") -> bool:
        return reference_time.weekday() in self.on_days


# ---------------------------------------------------------------------------
# Composite conditions (defined before AnyCondition, rebuilt after)
# ---------------------------------------------------------------------------

class AllOf(BaseModel):
    """All conditions must be met (logical AND)."""
    type:       Literal["all_of"] = "all_of"
    conditions: list[AnyCondition]  # resolved by model_rebuild() below

    def is_met(self, reference_time: datetime, cadence: "Cadence") -> bool:
        return all(c.is_met(reference_time, cadence) for c in self.conditions)


class AnyOf(BaseModel):
    """At least one condition must be met (logical OR)."""
    type:       Literal["any_of"] = "any_of"
    conditions: list[AnyCondition]  # resolved by model_rebuild() below

    def is_met(self, reference_time: datetime, cadence: "Cadence") -> bool:
        return any(c.is_met(reference_time, cadence) for c in self.conditions)


# ---------------------------------------------------------------------------
# Annotated union — the Pydantic field type for condition fields
# ---------------------------------------------------------------------------

AnyCondition = Annotated[
    TimeOfDayCondition
    | MinuteOfHourCondition
    | DayOfMonthCondition
    | DayOfWeekCondition
    | AllOf
    | AnyOf,
    Field(discriminator="type"),
]

# Resolve the forward reference `list[AnyCondition]` in AllOf / AnyOf.
AllOf.model_rebuild()
AnyOf.model_rebuild()

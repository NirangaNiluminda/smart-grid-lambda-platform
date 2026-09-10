"""Simulated clock.

The demo compresses time: one simulated business day passes in
``sim_day_seconds`` wall-clock seconds (default 600 = 10 minutes). Every service
that cares about "what simulated day is it" shares this one implementation so the
streaming source, the batch file drop and the Airflow trigger all agree.

Model
-----
``sim_epoch_wallclock`` is the real instant that maps to
``sim_start_date`` 00:00:00 UTC in simulated time. From there:

    elapsed_wall   = wall_now - sim_epoch_wallclock
    speedup        = 86_400 / sim_day_seconds
    sim_now        = sim_start_datetime + elapsed_wall * speedup

If ``sim_epoch_wallclock`` is not given, it is anchored the first time a clock is
constructed in the process (so ``docker compose up`` starts a fresh simulated
day 0).
"""

from __future__ import annotations

import datetime as dt

SECONDS_PER_DAY = 86_400.0


class SimClock:
    """Bi-directional wall-clock <-> simulated-clock converter.

    Satisfies :class:`energy_common.domain.ports.Clock`.
    """

    def __init__(
        self,
        *,
        sim_day_seconds: float,
        sim_start_date: dt.date,
        sim_epoch_wallclock: dt.datetime | None = None,
        _now: dt.datetime | None = None,
    ) -> None:
        if sim_day_seconds <= 0:
            raise ValueError("sim_day_seconds must be positive")
        self._sim_day_seconds = sim_day_seconds
        self._speedup = SECONDS_PER_DAY / sim_day_seconds
        self._sim_start = dt.datetime.combine(sim_start_date, dt.time.min, tzinfo=dt.UTC)
        anchor = sim_epoch_wallclock or _now or dt.datetime.now(dt.UTC)
        self._epoch_wall = anchor.astimezone(dt.UTC)

    # --- ports.Clock -------------------------------------------------------
    def now_wall(self) -> dt.datetime:
        return dt.datetime.now(dt.UTC)

    def now_sim(self) -> dt.datetime:
        return self.wall_to_sim(self.now_wall())

    # --- conversions -----------------------------------------------------
    def wall_to_sim(self, wall: dt.datetime) -> dt.datetime:
        elapsed = (wall.astimezone(dt.UTC) - self._epoch_wall).total_seconds()
        return self._sim_start + dt.timedelta(seconds=elapsed * self._speedup)

    def sim_to_wall(self, sim: dt.datetime) -> dt.datetime:
        sim_elapsed = (sim.astimezone(dt.UTC) - self._sim_start).total_seconds()
        return self._epoch_wall + dt.timedelta(seconds=sim_elapsed / self._speedup)

    # --- helpers -------------------------------------------------------
    @property
    def sim_day_wallclock_seconds(self) -> float:
        return self._sim_day_seconds

    def seconds_until_next_sim_midnight(self, *, at: dt.datetime | None = None) -> float:
        """Wall-clock seconds from ``at`` (now if omitted) to the next simulated
        midnight. Used by the batch file generator to schedule its drop."""
        sim_now = self.wall_to_sim(at) if at else self.now_sim()
        next_midnight = dt.datetime.combine(
            sim_now.date() + dt.timedelta(days=1), dt.time.min, tzinfo=dt.UTC
        )
        return (self.sim_to_wall(next_midnight) - (at or self.now_wall())).total_seconds()


def business_date_for(event_timestamp: dt.datetime) -> dt.date:
    """The simulated calendar date a reading belongs to.

    Simulated time is UTC, so the business date is just the UTC date of the
    simulated event timestamp. Centralised here so the partitioning rule has one
    definition (and one place to change if the utility ever bills on a local-time
    day boundary).
    """
    if event_timestamp.tzinfo is None:
        raise ValueError("event_timestamp must be timezone-aware")
    return event_timestamp.astimezone(dt.UTC).date()

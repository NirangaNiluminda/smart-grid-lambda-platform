"""BaseServiceSettings — the env-var parsing edge cases that bite in practice."""

from __future__ import annotations

import datetime as dt

from energy_common.config import BaseServiceSettings


def test_blank_sim_epoch_wallclock_env_value_parses_as_none(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """`.env.example` documents `SIM_EPOCH_WALLCLOCK=` (blank) as 'leave unset
    to anchor on process start' -- but a blank .env line sets the env var to
    "", not "absent". Every service reading exactly the .env the docs tell you
    to create (`cp .env.example .env`) would otherwise crash at startup trying
    to parse "" as a datetime, before doing anything else."""
    monkeypatch.setenv("SIM_EPOCH_WALLCLOCK", "")
    settings = BaseServiceSettings()
    assert settings.sim_epoch_wallclock is None


def test_explicit_sim_epoch_wallclock_still_parses(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("SIM_EPOCH_WALLCLOCK", "2026-01-01T00:00:00Z")
    settings = BaseServiceSettings()
    assert settings.sim_epoch_wallclock == dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


def test_unset_sim_epoch_wallclock_defaults_to_none(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("SIM_EPOCH_WALLCLOCK", raising=False)
    settings = BaseServiceSettings()
    assert settings.sim_epoch_wallclock is None

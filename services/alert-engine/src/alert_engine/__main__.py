"""Entrypoint for alert-engine service."""

from __future__ import annotations

import sys
import time

from energy_common import SimClock, get_logger

from alert_engine.composition import build_context_builder, build_engine
from alert_engine.config import load_settings

logger = get_logger("alert-engine")


def main() -> int:
    try:
        settings = load_settings()
        engine = build_engine(settings)
        context_builder = build_context_builder(settings)
        clock = SimClock(
            sim_day_seconds=settings.sim_day_seconds,
            sim_start_date=settings.sim_start_date,
            sim_epoch_wallclock=settings.sim_epoch_wallclock,
        )
        logger.info("alert_engine.started", interval=settings.evaluation_interval_seconds)
    except (KeyboardInterrupt, SystemExit):
        logger.info("alert_engine.stopped")
        return 0
    except Exception as exc:
        logger.exception("alert_engine.startup_failed", error=str(exc))
        return 1

    while True:
        try:
            now = clock.now_sim()
            bdate = now.date()
            ctx = context_builder.build(now=now, current_business_date=bdate)
            fired, resolved = engine.evaluate_cycle(ctx)
            if fired or resolved:
                logger.info("alert_engine.cycle", fired=len(fired), resolved=len(resolved))
        except (KeyboardInterrupt, SystemExit):
            logger.info("alert_engine.stopped")
            return 0
        except Exception as exc:
            # One bad cycle (a transient DB error, an unseeded dimension row,
            # Postgres/Prometheus briefly unreachable) must not take the whole
            # alert loop down -- log it and try again next cycle.
            logger.exception("alert_engine.cycle_failed", error=str(exc))
        time.sleep(settings.evaluation_interval_seconds)


if __name__ == "__main__":
    sys.exit(main())

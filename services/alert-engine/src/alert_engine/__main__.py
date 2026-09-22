"""Entrypoint for alert-engine service."""

from __future__ import annotations

import datetime as dt
import sys
import time

from energy_common import get_logger
from energy_common.simclock import business_date_for

from alert_engine.composition import build_engine
from alert_engine.config import load_settings
from alert_engine.domain.rules import RuleContext

logger = get_logger("alert-engine")


def main() -> int:
    try:
        settings = load_settings()
        engine = build_engine(settings)
        logger.info("alert_engine.started", interval=settings.evaluation_interval_seconds)

        while True:
            now = dt.datetime.now(dt.UTC)
            bdate = business_date_for(now)
            ctx = RuleContext(
                now=now,
                current_business_date=bdate,
            )
            engine.evaluate_cycle(ctx)
            time.sleep(settings.evaluation_interval_seconds)
    except (KeyboardInterrupt, SystemExit):
        logger.info("alert_engine.stopped")
        return 0
    except Exception as exc:
        logger.exception("alert_engine.fatal", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())

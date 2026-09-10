"""Process entrypoint: `python -m batch_file_generator`."""

from __future__ import annotations

import signal
import sys
from types import FrameType

from energy_common import configure_logging, get_logger, serve_metrics

from batch_file_generator.composition import build_loop
from batch_file_generator.config import load_settings


def main() -> int:
    settings = load_settings()
    configure_logging(level=settings.log_level, fmt=settings.log_format)
    log = get_logger(settings.service_name)
    serve_metrics(settings.metrics_port)

    loop = build_loop(settings)

    def _graceful(signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown.signal", signal=signal.Signals(signum).name)
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)

    log.info(
        "generator.start",
        landing_root=str(settings.landing_root),
        sim_day_seconds=settings.sim_day_seconds,
    )
    try:
        loop.run()
    except KeyboardInterrupt:
        log.info("generator.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())

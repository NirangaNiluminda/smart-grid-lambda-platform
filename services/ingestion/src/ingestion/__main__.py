"""Process entrypoint: `python -m ingestion`."""

from __future__ import annotations

import signal
import sys
from types import FrameType

from energy_common import configure_logging, get_logger, serve_metrics

from ingestion.composition import build_service
from ingestion.config import load_settings


def main() -> int:
    settings = load_settings()
    configure_logging(level=settings.log_level, fmt=settings.log_format)
    log = get_logger(settings.service_name)
    serve_metrics(settings.metrics_port)

    service = build_service(settings)

    def _graceful(signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown.signal", signal=signal.Signals(signum).name)
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)

    log.info("ingestion.start", lake_root=str(settings.lake_root))
    try:
        service.run()
    except KeyboardInterrupt:
        log.info("ingestion.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())

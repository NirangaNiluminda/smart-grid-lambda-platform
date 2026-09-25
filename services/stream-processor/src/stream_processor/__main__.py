"""Entrypoint: `python -m stream_processor` — the Lambda speed layer."""

from __future__ import annotations

import signal
import sys
from types import FrameType

from energy_common import configure_logging, get_logger, serve_metrics

from stream_processor.composition import build_pipeline
from stream_processor.config import load_settings


def main() -> int:
    settings = load_settings()
    configure_logging(level=settings.log_level, fmt=settings.log_format)
    log = get_logger(settings.service_name)
    serve_metrics(settings.metrics_port)

    pipeline = build_pipeline(settings)

    def _graceful(signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown.signal", signal=signal.Signals(signum).name)
        pipeline.stop()

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)

    log.info(
        "stream_processor.start",
        kafka_topic=settings.kafka_readings_topic,
        spark_master=settings.spark_master,
    )
    pipeline.start()
    try:
        pipeline.await_termination()
    except KeyboardInterrupt:
        pipeline.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Entrypoint for stream-processor service."""

from __future__ import annotations

import sys

from energy_common import get_logger

from stream_processor.composition import build_pipeline
from stream_processor.config import load_settings

logger = get_logger("stream-processor")


def main() -> int:
    try:
        settings = load_settings()
        pipeline = build_pipeline(settings)
        pipeline.run()
        return 0
    except Exception as exc:
        logger.exception("stream_processor.fatal", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())

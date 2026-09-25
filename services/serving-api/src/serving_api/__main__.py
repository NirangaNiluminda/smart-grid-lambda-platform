"""Entrypoint for serving-api service."""

from __future__ import annotations

import sys

import uvicorn
from energy_common import get_logger

from serving_api.composition import build_app
from serving_api.config import load_settings

logger = get_logger("serving-api")


def main() -> int:
    try:
        settings = load_settings()
        app = build_app(settings)
        logger.info("serving_api.starting", host=settings.api_host, port=settings.api_port)
        uvicorn.run(app, host=settings.api_host, port=settings.api_port)
        return 0
    except Exception as exc:
        logger.exception("serving_api.fatal", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())

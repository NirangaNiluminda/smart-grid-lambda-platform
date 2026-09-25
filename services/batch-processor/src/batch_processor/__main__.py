"""Entrypoint for batch-processor CLI."""

from __future__ import annotations

import argparse
import datetime as dt
import sys

from energy_common import get_logger

from batch_processor.composition import build_reconciliation_service
from batch_processor.config import load_settings

logger = get_logger("batch-processor")


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lambda batch layer reconciliation.")
    parser.add_argument(
        "--business-date",
        type=lambda s: dt.date.fromisoformat(s),
        required=True,
        help="Target business date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--allow-open",
        action="store_true",
        help="Allow processing if partition _SUCCESS marker is missing",
    )
    return parser.parse_args(args)


def main(args: list[str] | None = None) -> int:
    parsed = parse_args(args)
    try:
        settings = load_settings()
        service = build_reconciliation_service(settings)
        bills_count, summaries_count = service.run_reconciliation(
            business_date=parsed.business_date,
            allow_open=parsed.allow_open,
        )
        logger.info(
            "batch.completed",
            business_date=parsed.business_date.isoformat(),
            bills_written=bills_count,
            summaries_written=summaries_count,
        )
        return 0
    except Exception as exc:
        logger.exception("batch.failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())

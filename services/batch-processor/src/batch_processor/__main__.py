"""Entrypoint — NOT YET IMPLEMENTED (Part 2, Dev B). See docs/PART2_ONBOARDING.md §B2."""

from __future__ import annotations

import argparse
import sys

_GUIDANCE = """
batch-processor is a Part 2 skeleton.

  What it must do : Spark batch — read readings_raw/business_date=D, join the
                    tariff + weather feed, apply the billing Strategy, write
                    daily_household_billing + daily_zone_solar_summary + a report.
  Where to start  : docs/PART2_ONBOARDING.md  §B2
  Contract        : docs/DATA_CONTRACTS.md    §2, §3 (in) and §4 (out)
  Reuse           : energy_common.domain.transforms.energy_balance
"""


def main() -> int:
    parser = argparse.ArgumentParser(prog="batch-processor")
    parser.add_argument("--business-date", required=True, help="YYYY-MM-DD of the closed day")
    parser.add_argument(
        "--allow-open", action="store_true", help="proceed without partition _SUCCESS"
    )
    parser.parse_args()
    print(_GUIDANCE, file=sys.stderr)
    raise NotImplementedError("batch-processor: implement application/reconcile.py (Part 2 §B2)")


if __name__ == "__main__":
    sys.exit(main())

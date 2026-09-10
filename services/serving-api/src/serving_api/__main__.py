"""Entrypoint — NOT YET IMPLEMENTED (Part 2, Dev B). See docs/PART2_ONBOARDING.md §B4."""

from __future__ import annotations

import sys

_GUIDANCE = """
serving-api is a Part 2 skeleton.

  What it must do : read-only FastAPI over Postgres; merge speed (rt_zone_metrics)
                    and batch (daily_household_billing) views; /healthz; /metrics.
  Where to start  : docs/PART2_ONBOARDING.md  §B4
  Endpoints       : services/serving-api/README.md
"""


def main() -> int:
    print(_GUIDANCE, file=sys.stderr)
    raise NotImplementedError("serving-api: implement app.py + api/ (Part 2 §B4)")


if __name__ == "__main__":
    sys.exit(main())

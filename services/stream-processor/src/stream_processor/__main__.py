"""Entrypoint — NOT YET IMPLEMENTED (Part 2, Dev B).

This skeleton exists so the service has a home, a config class, and a place for
tests. See docs/PART2_ONBOARDING.md §B1 for the build plan and acceptance
criteria. Delete this message as you implement `application/pipeline.py`.
"""

from __future__ import annotations

import sys

_GUIDANCE = """
stream-processor is a Part 2 skeleton.

  What it must do : Spark Structured Streaming — consume meter.readings,
                    validate/enrich/window per zone, upsert rt_zone_metrics.
  Where to start  : docs/PART2_ONBOARDING.md  §B1
  Contract        : docs/DATA_CONTRACTS.md    §1 (in) and §4 (out)
  Reuse           : energy_common.domain.transforms  (shared with the batch layer)
"""


def main() -> int:
    print(_GUIDANCE, file=sys.stderr)
    raise NotImplementedError("stream-processor: implement application/pipeline.py (Part 2 §B1)")


if __name__ == "__main__":
    sys.exit(main())

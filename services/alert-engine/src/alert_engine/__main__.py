"""Entrypoint — NOT YET IMPLEMENTED (Part 2, Dev B). See docs/PART2_ONBOARDING.md §B5."""

from __future__ import annotations

import sys

_GUIDANCE = """
alert-engine is a Part 2 skeleton.

  What it must do : evaluate alert-rule Strategies over Postgres + Prometheus on a
                    schedule; dedupe; fan out to DB / log / webhook sinks.
  Where to start  : docs/PART2_ONBOARDING.md  §B5
  Rules           : docs/OBSERVABILITY.md     §3
"""


def main() -> int:
    print(_GUIDANCE, file=sys.stderr)
    raise NotImplementedError("alert-engine: implement application/evaluator.py (Part 2 §B5)")


if __name__ == "__main__":
    sys.exit(main())

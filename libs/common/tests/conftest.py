from __future__ import annotations

import pytest
from energy_common.observability import logging as _logging


@pytest.fixture(autouse=True)
def _fresh_logging_state() -> None:
    """Each test starts with logging unconfigured so it can set its own format
    and so a cached stream from a previous test cannot leak in."""
    _logging._reset_for_tests()

"""Makes `dags` importable regardless of pytest's rootdir or import-mode.

Without this, `orchestration/airflow/tests/test_dag.py`'s `from dags.
daily_energy_reconciliation import ...` only resolves when pytest happens to be
invoked from inside `orchestration/airflow/` — which `make test` / CI never do,
so the DAG helper tests silently never ran. See `docs/DATA_CONTRACTS.md` /
PLAN.md's B3 note.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

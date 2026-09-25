"""Ties the orchestration layer to the simulated clock.

`daily_energy_reconciliation` is externally triggered (`schedule=None`), not
polled on a wall-clock schedule, because nothing about a wall-clock schedule
lines up with a demo session where a simulated day closes every
`SIM_DAY_SECONDS`. This adapter is the trigger: the moment
`batch-file-generator` finishes writing a business day's feed (the `_SUCCESS`
marker is the last thing it writes), it asks Airflow to run the DAG for that
exact business date.

Best-effort by design: `batch-file-generator` is a Part 1 service and must keep
working standalone when Part 2 (Airflow) isn't deployed (`make up`, no
`docker-compose.part2.yml`). A failed or refused trigger call is logged and
swallowed — see `docs/RUNBOOK.md` for the manual fallback
(`airflow dags trigger`).
"""

from __future__ import annotations

import datetime as dt
from typing import Protocol

import httpx
from energy_common import get_logger

logger = get_logger("batch-file-generator")


class DagTrigger(Protocol):
    def trigger(self, business_date: dt.date) -> None: ...


class NullDagTrigger:
    """Used when AIRFLOW_TRIGGER_ENABLED is false (the default) — e.g. Part 1 only."""

    def trigger(self, business_date: dt.date) -> None:
        return None


class AirflowDagTrigger:
    def __init__(
        self,
        *,
        base_url: str,
        dag_id: str,
        username: str,
        password: str,
        timeout_s: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._dag_id = dag_id
        self._auth = (username, password)
        self._timeout_s = timeout_s

    def trigger(self, business_date: dt.date) -> None:
        logical_date = dt.datetime.combine(business_date, dt.time.min, tzinfo=dt.UTC)
        run_id = f"triggered__{business_date.isoformat()}"
        payload = {
            "logical_date": logical_date.isoformat(),
            "dag_run_id": run_id,
        }
        try:
            resp = httpx.post(
                f"{self._base_url}/api/v1/dags/{self._dag_id}/dagRuns",
                json=payload,
                auth=self._auth,
                timeout=self._timeout_s,
            )
            if resp.status_code == 409:
                logger.info(
                    "airflow.trigger.already_running", business_date=business_date.isoformat()
                )
                return
            resp.raise_for_status()
            logger.info(
                "airflow.trigger.ok", business_date=business_date.isoformat(), dag_run_id=run_id
            )
        except Exception:
            logger.warning(
                "airflow.trigger.failed", business_date=business_date.isoformat(), exc_info=True
            )

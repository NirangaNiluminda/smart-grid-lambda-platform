from __future__ import annotations

import datetime as dt

import httpx
from batch_file_generator.adapters.airflow_trigger import AirflowDagTrigger, NullDagTrigger


def test_null_trigger_is_a_no_op() -> None:
    NullDagTrigger().trigger(dt.date(2026, 1, 1))  # must not raise


def test_posts_the_expected_dag_run_payload(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = request.read()
        return httpx.Response(200, json={"dag_run_id": "triggered__2026-01-01"})

    def fake_post(url, *, json, auth, timeout):  # type: ignore[no-untyped-def]
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            return client.post(url, json=json, auth=auth, timeout=timeout)

    monkeypatch.setattr(httpx, "post", fake_post)
    trigger = AirflowDagTrigger(
        base_url="http://airflow:8080",
        dag_id="daily_energy_reconciliation",
        username="admin",
        password="admin",
    )
    trigger.trigger(dt.date(2026, 1, 1))

    assert captured["url"].endswith("/api/v1/dags/daily_energy_reconciliation/dagRuns")
    assert b"2026-01-01" in captured["body"]


def test_already_running_409_is_treated_as_success(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409)

    def fake_post(url, *, json, auth, timeout):  # type: ignore[no-untyped-def]
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            return client.post(url, json=json, auth=auth, timeout=timeout)

    monkeypatch.setattr(httpx, "post", fake_post)
    trigger = AirflowDagTrigger(
        base_url="http://airflow:8080", dag_id="d", username="a", password="b"
    )
    trigger.trigger(dt.date(2026, 1, 1))  # must not raise


def test_connection_failure_is_swallowed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def fake_post(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "post", fake_post)
    trigger = AirflowDagTrigger(
        base_url="http://airflow:8080", dag_id="d", username="a", password="b"
    )
    trigger.trigger(dt.date(2026, 1, 1))  # must not raise

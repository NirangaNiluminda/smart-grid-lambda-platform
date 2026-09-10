"""Structured logging emits parseable JSON with the mandatory fields."""

from __future__ import annotations

import json

from energy_common.observability import bind_context, clear_context, configure_logging, get_logger


def test_logger_emits_json_with_mandatory_fields(capsys) -> None:  # type: ignore[no-untyped-def]
    configure_logging(level="INFO", fmt="json")
    log = get_logger("test-service")
    log.info("reading.published", grid_zone="zone-north", records_out=3)

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["service"] == "test-service"
    assert payload["event"] == "reading.published"
    assert payload["level"] == "info"
    assert payload["grid_zone"] == "zone-north"
    assert payload["records_out"] == 3
    assert "ts" in payload


def test_bound_context_appears_on_later_lines(capsys) -> None:  # type: ignore[no-untyped-def]
    configure_logging()
    log = get_logger("ctx-service")
    bind_context(business_date="2026-01-01", correlation_id="abc123")
    try:
        log.info("partition.flushed")
        payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert payload["business_date"] == "2026-01-01"
        assert payload["correlation_id"] == "abc123"
    finally:
        clear_context()

"""Composition root for batch-processor."""

from __future__ import annotations

from energy_common.observability import serve_metrics
from energy_common.storage import pg_engine

from batch_processor.adapters.reports import ReportRenderer
from batch_processor.adapters.repository import PostgresBatchRepository
from batch_processor.application.reconcile import BatchReconciliationService
from batch_processor.config import Settings


def build_reconciliation_service(settings: Settings) -> BatchReconciliationService:
    serve_metrics(settings.metrics_port)
    engine = pg_engine(settings.pg_dsn)
    repo = PostgresBatchRepository(engine)
    renderer = ReportRenderer(settings.report_out_dir)
    return BatchReconciliationService(
        settings=settings,
        repository=repo,
        report_renderer=renderer,
    )

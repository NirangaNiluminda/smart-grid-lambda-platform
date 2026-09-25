"""Composition root for serving-api."""

from __future__ import annotations

from energy_common.storage import pg_engine
from fastapi import FastAPI

from serving_api.adapters.repository import PostgresServingRepository
from serving_api.application.api import create_app
from serving_api.config import Settings


def build_app(settings: Settings) -> FastAPI:
    engine = pg_engine(settings.pg_dsn)
    repo = PostgresServingRepository(engine)
    return create_app(repo, settings)

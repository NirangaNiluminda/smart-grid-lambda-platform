# syntax=docker/dockerfile:1
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# The shared kernel is a sibling package; copy both and install editable.
COPY libs/common /app/libs/common
COPY services/meter-simulator /app/services/meter-simulator

RUN pip install --upgrade pip \
 && pip install "/app/libs/common[kafka]" \
 && pip install /app/services/meter-simulator

# Seed data is mounted read-only at runtime via compose; this is the fallback path.
COPY data/seed /app/data/seed

EXPOSE 9101
ENTRYPOINT ["python", "-m", "meter_simulator"]

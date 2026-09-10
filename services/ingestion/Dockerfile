# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY libs/common /app/libs/common
COPY services/ingestion /app/services/ingestion

RUN pip install --upgrade pip \
 && pip install "/app/libs/common[kafka,parquet]" \
 && pip install /app/services/ingestion

EXPOSE 9102
ENTRYPOINT ["python", "-m", "ingestion"]

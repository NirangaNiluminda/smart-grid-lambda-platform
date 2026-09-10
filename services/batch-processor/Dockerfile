# syntax=docker/dockerfile:1
# Part 2 skeleton image. Dev B: add any service-specific system deps (e.g. a JRE
# for Spark on stream-processor / batch-processor) as the implementation lands.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY libs/common /app/libs/common
COPY services/batch-processor /app/services/batch-processor

RUN pip install --upgrade pip \
 && pip install "/app/libs/common[postgres,parquet]" \
 && pip install /app/services/batch-processor

EXPOSE 9112
ENTRYPOINT ["python", "-m", "batch_processor"]

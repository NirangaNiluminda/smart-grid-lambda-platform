# syntax=docker/dockerfile:1
# Part 2 skeleton image. Dev B: add any service-specific system deps (e.g. a JRE
# for Spark on stream-processor / batch-processor) as the implementation lands.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY libs/common /app/libs/common
COPY services/alert-engine /app/services/alert-engine

RUN pip install --upgrade pip \
 && pip install "/app/libs/common[postgres]" \
 && pip install /app/services/alert-engine

EXPOSE 9114
ENTRYPOINT ["python", "-m", "alert_engine"]

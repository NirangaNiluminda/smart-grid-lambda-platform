# syntax=docker/dockerfile:1
# Part 2 skeleton image. Dev B: add any service-specific system deps (e.g. a JRE
# for Spark on stream-processor / batch-processor) as the implementation lands.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY libs/common /app/libs/common
COPY services/stream-processor /app/services/stream-processor

RUN pip install --upgrade pip \
 && pip install "/app/libs/common[kafka,postgres]" \
 && pip install /app/services/stream-processor

EXPOSE 9111
ENTRYPOINT ["python", "-m", "stream_processor"]

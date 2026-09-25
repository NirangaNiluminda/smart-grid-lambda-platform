# syntax=docker/dockerfile:1
# Spark's batch reader needs a JVM; python:3.11-slim doesn't ship one.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY libs/common /app/libs/common
COPY services/batch-processor /app/services/batch-processor

RUN pip install --upgrade pip \
 && pip install "/app/libs/common[postgres]" \
 && pip install /app/services/batch-processor

EXPOSE 9112
ENTRYPOINT ["python", "-m", "batch_processor"]

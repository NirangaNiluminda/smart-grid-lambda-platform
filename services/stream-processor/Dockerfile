# syntax=docker/dockerfile:1
# Spark Structured Streaming needs a JVM; python:3.11-slim doesn't ship one.
# Pinned to -bookworm (Debian 12): the unpinned python:3.11-slim tag now
# resolves to Debian trixie, which dropped the openjdk-17-jre-headless
# package (only openjdk-21 is available there) -- pinning avoids the build
# silently breaking again the next time Debian's default rolls forward.
FROM python:3.11-slim-bookworm

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
COPY services/stream-processor /app/services/stream-processor

RUN pip install --upgrade pip \
 && pip install "/app/libs/common[postgres]" \
 && pip install /app/services/stream-processor

EXPOSE 9111
ENTRYPOINT ["python", "-m", "stream_processor"]

# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY libs/common /app/libs/common
COPY services/batch-file-generator /app/services/batch-file-generator

RUN pip install --upgrade pip \
 && pip install /app/libs/common \
 && pip install /app/services/batch-file-generator

COPY data/seed /app/data/seed

EXPOSE 9103
ENTRYPOINT ["python", "-m", "batch_file_generator"]

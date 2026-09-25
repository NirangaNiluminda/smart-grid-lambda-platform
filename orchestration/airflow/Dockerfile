# syntax=docker/dockerfile:1
# Custom Airflow image: the stock apache/airflow image has no idea about our
# packages. The `run_batch` task in daily_energy_reconciliation.py imports
# `batch_processor` directly (in-process, not via spark-submit/subprocess), so
# it has to be installed here, along with a JVM for the Spark batch job it runs.
#
# `--no-deps` is deliberate: Airflow 2.10 pins its own SQLAlchemy/psycopg2
# versions, which our packages must not fight. energy_common's SQL is plain
# SQLAlchemy Core (`text()` + `create_engine()`), which runs fine against
# whatever SQLAlchemy/psycopg2 Airflow already ships.
FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

USER airflow
COPY --chown=airflow:root libs/common /opt/airflow/deps/libs/common
COPY --chown=airflow:root services/batch-processor /opt/airflow/deps/services/batch-processor

RUN pip install --no-cache-dir --no-deps \
      /opt/airflow/deps/libs/common \
      /opt/airflow/deps/services/batch-processor \
 && pip install --no-cache-dir "pyspark>=3.5,<3.6" pydantic-settings structlog prometheus-client

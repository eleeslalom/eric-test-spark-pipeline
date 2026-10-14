# Bookworm keeps Java 17 available on both arm64 and amd64.
FROM python:3.11-slim-bookworm AS runtime
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java \
    PYTHONPATH=/opt/project \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=python \
    DATA_DIR=/data
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /opt/project
COPY . /opt/project

# This target is built only when optional Airflow services are requested.
FROM apache/airflow:2.10.5-python3.11 AS airflow
USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java \
    PYTHONPATH=/opt/project \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=python \
    DATA_DIR=/data
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/requirements.txt
WORKDIR /opt/project
COPY --chown=airflow:root . /opt/project

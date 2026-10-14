# Bookworm keeps Java 17 available on both arm64 and amd64.
FROM python:3.11-slim-bookworm
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYSPARK_PYTHON=python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /opt/project
COPY jobs /opt/project/jobs
CMD ["spark-submit", "jobs/main.py"]

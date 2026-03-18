FROM apache/airflow:2.10.0-python3.11

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    ca-certificates \
    curl \
    procps \
    bash \
    tini \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
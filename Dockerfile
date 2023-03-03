FROM apache/airflow:2.5.0-python3.8

LABEL maintainer="stefen Taime"

USER root:root

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

USER airflow

RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow

WORKDIR /opt/airflow

RUN pip install -r requirements.txt
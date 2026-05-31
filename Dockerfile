FROM apache/airflow:2.9.3-python3.11

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

USER root
RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
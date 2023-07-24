FROM apache/airflow:latest-python3.9
ENV PYTHONPATH=/opt/airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
FROM apache/airflow:2.8.1-python3.10

USER root
# Install git (required for dbt package dependencies)
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

USER airflow

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

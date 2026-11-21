FROM apache/airflow:3.3.1-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir "pyspark>=3.5,<4" "pandas>=2,<3" "matplotlib>=3.7,<4" "pytest>=7,<10"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/opt/project

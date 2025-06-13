FROM apache/airflow:2.7.3-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless \
        curl \
        wget \
        procps \
        postgresql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

# Install additional Airflow providers and PySpark as airflow user
RUN pip install --no-cache-dir --user \
    apache-airflow-providers-postgres==5.6.1 \
    apache-airflow-providers-apache-spark==4.1.3 \
    pyspark==3.4.1 \
    psycopg2-binary==2.9.7 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    matplotlib==3.7.2 \
    seaborn==0.12.2

# Update PATH to include user's local bin directory
ENV PATH="/home/airflow/.local/bin:$PATH"

# Set working directory
WORKDIR /opt/airflow 
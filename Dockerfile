# Dockerfile
FROM apache/airflow:2.8.1

# Switch to the airflow user
USER airflow

# Install additional dependencies for Apache Airflow and Spark
RUN pip install --no-cache-dir apache-airflow[apache.spark]==2.8.1

# Install Java (OpenJDK) for Spark
USER root
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

# Switch back to the airflow user
USER airflow

# Set the working directory
WORKDIR /usr/local/airflow

# Copy requirements.txt and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Copy the remaining application files
COPY . /usr/local/airflow

# Copy data directory
# COPY ./airflow/plugins/lib/data /data

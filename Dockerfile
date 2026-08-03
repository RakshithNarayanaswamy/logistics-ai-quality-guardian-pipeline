FROM python:3.12-slim

# Java is required by PySpark (Spark 4.x needs Java 17+; Debian bookworm's
# default JRE is OpenJDK 17).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV AIRFLOW_HOME=/opt/project/airflow \
    PYTHONPATH=/opt/project

EXPOSE 8080

# `airflow standalone` initializes the metadata DB on first boot, creates an
# admin user (credentials printed in the logs), and runs scheduler + webserver.
CMD ["airflow", "standalone"]

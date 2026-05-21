# ─── Flink Python Job Image ───────────────────────────────────────────────────
# Extends the official Flink image with PyFlink and IoTDB client.
FROM flink:1.17-scala_2.12-java11

# Install Python 3.11 and pip
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Make python3.11 the default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/python  python  /usr/bin/python3.11 1

WORKDIR /opt/flink/usrlib

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline_job.py .

# Flink will submit this job on container start
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
    KAFKA_TOPIC=sensor-topic \
    KAFKA_DEAD_LETTER_TOPIC=sensor-topic-dlq \
    KAFKA_GROUP_ID=flink-pipeline \
    IOTDB_HOST=iotdb \
    IOTDB_PORT=6667 \
    IOTDB_USER=root \
    IOTDB_PASSWORD=root \
    IOTDB_STORAGE_GROUP=root.factory1 \
    CHECKPOINT_DIR=/tmp/flink-checkpoints

CMD ["python3", "pipeline_job.py"]

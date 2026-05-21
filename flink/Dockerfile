FROM flink:1.17.2-scala_2.12-java11

USER root

# Install system dependencies required by PyFlink
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    python-is-python3 \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink

# Upgrade pip tooling first
RUN pip3 install --upgrade pip setuptools wheel

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy Flink job
COPY pipeline_job.py /opt/flink/usrlib/pipeline_job.py

# Install Kafka connector + Kafka client jars
RUN mkdir -p /opt/flink/lib && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar

USER flink
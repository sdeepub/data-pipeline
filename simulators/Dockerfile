# ─── Stage 1: base ───────────────────────────────────────────────────────────
FROM python:3.11-slim AS base

# Keeps Python from buffering stdout/stderr (important for Docker logs)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# ─── Stage 2: dependencies ───────────────────────────────────────────────────
FROM base AS deps

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ─── Stage 3: final image ────────────────────────────────────────────────────
FROM base AS final

# Copy installed packages from deps stage (keeps final image lean)
COPY --from=deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

COPY machine_simulator.py .

# Runtime defaults — all overridable via docker-compose or k8s env vars
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
    KAFKA_TOPIC=sensor-topic \
    NUM_MACHINES=100 \
    SEND_INTERVAL_SEC=2 \
    MACHINE_PREFIX=MC

# Run as non-root user (security best practice)
RUN useradd -m simulator
USER simulator

CMD ["python", "machine_simulator.py"]

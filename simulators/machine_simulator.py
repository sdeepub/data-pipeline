"""
IoT Machine Data Simulator
Simulates 100 industrial machines streaming sensor telemetry to Kafka.

Each machine runs in its own thread, publishing independently with slight
timing jitter to simulate real-world behaviour.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS  default: kafka:29092
  KAFKA_TOPIC              default: sensor-topic
  NUM_MACHINES             default: 100
  SEND_INTERVAL_SEC        default: 2
  MACHINE_PREFIX           default: MC
"""

import json
import time
import random
import os
import signal
import sys
import threading
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─── Config (from environment, with safe defaults) ───────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "sensor-topic")
NUM_MACHINES            = int(os.getenv("NUM_MACHINES", "100"))
SEND_INTERVAL_SEC       = float(os.getenv("SEND_INTERVAL_SEC", "2"))
MACHINE_PREFIX          = os.getenv("MACHINE_PREFIX", "MC")

# ─── Machine Metadata ────────────────────────────────────────────────────────
# Assign each machine a fixed type and location for realistic enrichment

MACHINE_TYPES = ["compressor", "turbine", "pump", "generator", "conveyor"]
LOCATIONS     = ["zone_A", "zone_B", "zone_C", "zone_D"]

def build_machine_registry(n: int) -> dict:
    """Create a static registry of machine metadata."""
    registry = {}
    for i in range(1, n + 1):
        machine_id = f"{MACHINE_PREFIX}{i:03d}"
        registry[machine_id] = {
            "type":     MACHINE_TYPES[i % len(MACHINE_TYPES)],
            "location": LOCATIONS[i % len(LOCATIONS)],
        }
    return registry

MACHINES = build_machine_registry(NUM_MACHINES)

# ─── Fault Injection ─────────────────────────────────────────────────────────
# 2% chance per reading that a machine enters a fault state

FAULT_PROBABILITY = 0.02

FAULT_PROFILES = {
    "overtemp":       {"gas_temperature": (470, 520), "gas_pressure": (4.5, 6.0), "fault_code": "F001"},
    "overpressure":   {"gas_temperature": (380, 450), "gas_pressure": (6.5, 8.0), "fault_code": "F002"},
    "high_vibration": {"spin_rate": (3500, 4200),     "torque": (18, 25),          "fault_code": "F003"},
    "low_humidity":   {"humidity": (5, 15),                                         "fault_code": "F004"},
}

def get_fault() -> dict | None:
    """Return a fault profile dict, or None if no fault this cycle."""
    if random.random() < FAULT_PROBABILITY:
        profile_name = random.choice(list(FAULT_PROFILES.keys()))
        return FAULT_PROFILES[profile_name]
    return None

# ─── Data Generation ─────────────────────────────────────────────────────────

def generate_reading(machine_id: str) -> dict:
    """Generate one sensor reading for a machine, with optional fault injection."""
    meta  = MACHINES[machine_id]
    fault = get_fault()

    # Base (normal) ranges
    gas_temperature = round(random.uniform(380, 450), 2)
    gas_pressure    = round(random.uniform(4.5, 6.0), 2)
    humidity        = round(random.uniform(30, 60), 2)
    spin_rate       = round(random.uniform(2500, 3200), 2)
    torque          = round(random.uniform(10, 15), 2)
    fault_code      = None
    status          = "running"

    # Override with fault values if a fault is active
    if fault:
        status     = "fault"
        fault_code = fault.get("fault_code")
        if "gas_temperature" in fault:
            gas_temperature = round(random.uniform(*fault["gas_temperature"]), 2)
        if "gas_pressure" in fault:
            gas_pressure    = round(random.uniform(*fault["gas_pressure"]), 2)
        if "humidity" in fault:
            humidity        = round(random.uniform(*fault["humidity"]), 2)
        if "spin_rate" in fault:
            spin_rate       = round(random.uniform(*fault["spin_rate"]), 2)
        if "torque" in fault:
            torque          = round(random.uniform(*fault["torque"]), 2)

    return {
        "machine_id":      machine_id,
        "machine_type":    meta["type"],
        "location":        meta["location"],
        "timestamp":       int(time.time() * 1000),   # epoch ms
        "gas_temperature": gas_temperature,            # °C
        "gas_pressure":    gas_pressure,               # bar
        "humidity":        humidity,                   # %RH
        "spin_rate":       spin_rate,                  # RPM
        "torque":          torque,                     # Nm
        "status":          status,
        "fault_code":      fault_code,
    }

# ─── Kafka Producer (shared across threads) ──────────────────────────────────

def create_producer(retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    """Create a Kafka producer, retrying until Kafka is ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",               # wait for all replicas
                retries=5,
                max_block_ms=10_000,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            log.warning(f"Kafka not ready (attempt {attempt}/{retries}): {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
    log.error("Could not connect to Kafka after multiple attempts. Exiting.")
    sys.exit(1)

# ─── Machine Thread ──────────────────────────────────────────────────────────

stop_event = threading.Event()

def run_machine(machine_id: str, producer: KafkaProducer) -> None:
    """Thread target: continuously send readings for one machine."""
    # Jitter: stagger machine start times so they don't all fire at once
    jitter = random.uniform(0, SEND_INTERVAL_SEC)
    time.sleep(jitter)

    sent_count = 0
    while not stop_event.is_set():
        try:
            data = generate_reading(machine_id)
            producer.send(KAFKA_TOPIC, value=data)
            sent_count += 1

            # Log every 50 messages per machine (avoid log spam with 100 machines)
            if sent_count % 50 == 1:
                log.info(f"[{machine_id}] #{sent_count} sent | status={data['status']} "
                         f"temp={data['gas_temperature']} pressure={data['gas_pressure']}")

            if data["fault_code"]:
                log.warning(f"[{machine_id}] FAULT detected: {data['fault_code']} at {data['location']}")

        except KafkaError as e:
            log.error(f"[{machine_id}] Kafka send error: {e}")

        # Sleep with slight per-cycle jitter (±10%) to avoid thundering herd
        sleep_time = SEND_INTERVAL_SEC * random.uniform(0.9, 1.1)
        stop_event.wait(sleep_time)

# ─── Graceful Shutdown ───────────────────────────────────────────────────────

def handle_signal(sig, frame):
    log.info(f"Received signal {sig}. Shutting down gracefully...")
    stop_event.set()

signal.signal(signal.SIGINT,  handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    log.info(f"Starting IoT simulator: {NUM_MACHINES} machines → topic '{KAFKA_TOPIC}' @ {KAFKA_BOOTSTRAP_SERVERS}")
    log.info(f"Send interval: {SEND_INTERVAL_SEC}s | Fault probability: {FAULT_PROBABILITY*100:.0f}%")

    producer = create_producer()

    threads = []
    for machine_id in MACHINES:
        t = threading.Thread(
            target=run_machine,
            args=(machine_id, producer),
            name=machine_id,
            daemon=True
        )
        t.start()
        threads.append(t)

    log.info(f"All {NUM_MACHINES} machine threads started.")

    # Wait until shutdown signal
    while not stop_event.is_set():
        time.sleep(1)

    log.info("Stopping all machine threads...")
    for t in threads:
        t.join(timeout=5)

    producer.flush()
    producer.close()
    log.info("Simulator stopped cleanly.")

if __name__ == "__main__":
    main()

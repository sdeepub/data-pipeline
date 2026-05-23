"""
Flink Pipeline Job — Kafka → IoTDB
====================================
Consumes machine telemetry from Kafka, processes it in real-time,
and writes results to Apache IoTDB.

Processing stages:
  1. Consume JSON messages from Kafka topic
  2. Parse & validate each message
  3. Route malformed messages to a dead-letter topic
  4. Compute 1-minute tumbling window aggregations per machine
  5. Detect anomalies (threshold breaches)
  6. Write processed records + aggregations to IoTDB

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   default: kafka:29092
  KAFKA_TOPIC               default: sensor-topic
  KAFKA_DEAD_LETTER_TOPIC   default: sensor-topic-dlq
  KAFKA_GROUP_ID            default: flink-pipeline
  IOTDB_HOST                default: iotdb
  IOTDB_PORT                default: 6667
  IOTDB_USER                default: root
  IOTDB_PASSWORD            default: root
  IOTDB_STORAGE_GROUP       default: root.factory1
  CHECKPOINT_DIR            default: /tmp/flink-checkpoints
"""

import os
import json
import logging
import time
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
# FIX: Import TimestampAssigner base class for a serializable implementation.
#      An inline lambda cannot be pickled by the Flink distributed runtime
#      and raises PicklingError when the JobManager ships tasks to remote
#      TaskManagers.  A named class is fully serializable.
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import (
    MapFunction, FilterFunction, ProcessWindowFunction, FlatMapFunction
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("flink-pipeline")

# ─── Config ──────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "sensor-topic")
KAFKA_DLQ_TOPIC         = os.getenv("KAFKA_DEAD_LETTER_TOPIC", "sensor-topic-dlq")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "flink-pipeline")
IOTDB_HOST              = os.getenv("IOTDB_HOST", "iotdb")
IOTDB_PORT              = int(os.getenv("IOTDB_PORT", "6667"))
IOTDB_USER              = os.getenv("IOTDB_USER", "root")
IOTDB_PASSWORD          = os.getenv("IOTDB_PASSWORD", "root")
IOTDB_STORAGE_GROUP     = os.getenv("IOTDB_STORAGE_GROUP", "root.factory1")
CHECKPOINT_DIR          = os.getenv("CHECKPOINT_DIR", "/tmp/flink-checkpoints")

# ─── Anomaly Thresholds ──────────────────────────────────────────────────────

THRESHOLDS = {
    "gas_temperature": {"min": 350.0, "max": 460.0},
    "gas_pressure":    {"min": 4.0,   "max": 6.5},
    "humidity":        {"min": 20.0,  "max": 65.0},
    "spin_rate":       {"min": 2000,  "max": 3500},
    "torque":          {"min": 8.0,   "max": 18.0},
}

# ─── Required IoTDB schema ───────────────────────────────────────────────────

MEASUREMENTS = ["gas_temperature", "gas_pressure", "humidity", "spin_rate", "torque"]
DATA_TYPES   = [TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT]
ENCODINGS    = [TSEncoding.GORILLA] * len(MEASUREMENTS)    # GORILLA is optimal for float sensor data
COMPRESSORS  = [Compressor.SNAPPY] * len(MEASUREMENTS)

# ─── IoTDB helper ────────────────────────────────────────────────────────────

def get_iotdb_session() -> Session:
    """Open and return an authenticated IoTDB session."""
    session = Session(IOTDB_HOST, IOTDB_PORT, IOTDB_USER, IOTDB_PASSWORD)
    session.open(enable_rpc_compression=False)
    return session


def ensure_schema(session: Session, machine_ids: list[str]) -> None:
    """
    Idempotently create storage group and time series for all machines.
    Safe to call on every startup — IoTDB ignores duplicates.
    """
    try:
        session.set_storage_group(IOTDB_STORAGE_GROUP)
    except Exception:
        pass  # Already exists

    for machine_id in machine_ids:
        device_path = f"{IOTDB_STORAGE_GROUP}.{machine_id}"
        paths = [f"{device_path}.{m}" for m in MEASUREMENTS]
        try:
            session.create_multi_time_series(
                paths_list=paths,
                data_type_lst=DATA_TYPES,
                encoding_lst=ENCODINGS,
                compressor_lst=COMPRESSORS,
            )
            log.info(f"Schema ensured for {device_path}")
        except Exception:
            pass  # Time series already exist


def write_record_to_iotdb(session: Session, record: dict) -> None:
    """Write a single validated record to IoTDB."""
    device_path = f"{IOTDB_STORAGE_GROUP}.{record['machine_id']}"
    session.insert_record(
        device_id=device_path,
        timestamp=record["timestamp"],
        measurements=MEASUREMENTS,
        data_types=DATA_TYPES,
        values=[
            float(record["gas_temperature"]),
            float(record["gas_pressure"]),
            float(record["humidity"]),
            float(record["spin_rate"]),
            float(record["torque"]),
        ]
    )


def write_aggregation_to_iotdb(session: Session, agg: dict) -> None:
    """Write a windowed aggregation record to IoTDB under a separate path."""
    device_path = f"{IOTDB_STORAGE_GROUP}_agg.{agg['machine_id']}"
    agg_measurements = [
        "avg_gas_temperature", "max_gas_temperature", "min_gas_temperature",
        "avg_gas_pressure",    "max_gas_pressure",
        "avg_spin_rate",       "max_spin_rate",
        "avg_torque",
        "fault_count",         "reading_count",
    ]
    agg_types  = [TSDataType.FLOAT] * 8 + [TSDataType.INT32, TSDataType.INT32]
    agg_values = [
        float(agg["avg_gas_temperature"]),
        float(agg["max_gas_temperature"]),
        float(agg["min_gas_temperature"]),
        float(agg["avg_gas_pressure"]),
        float(agg["max_gas_pressure"]),
        float(agg["avg_spin_rate"]),
        float(agg["max_spin_rate"]),
        float(agg["avg_torque"]),
        int(agg["fault_count"]),
        int(agg["reading_count"]),
    ]
    session.insert_record(
        device_id=device_path,
        timestamp=agg["window_end_ts"],
        measurements=agg_measurements,
        data_types=agg_types,
        values=agg_values,
    )

# ─── Timestamp Assigner ───────────────────────────────────────────────────────

# FIX: Replace the inline lambda with a named class that Flink can pickle
#      and ship to remote TaskManagers in distributed execution.
#
#      The original code used:
#        .with_timestamp_assigner(
#            lambda event, _: json.loads(event).get("timestamp", int(time.time() * 1000))
#        )
#      Python lambdas are NOT reliably serializable across processes — Flink's
#      distributed task dispatch calls pickle.dumps() on all operators, and
#      lambdas that close over module-level names can fail with PicklingError.
#      A named top-level class is always safely serializable.
class KafkaEventTimestampAssigner(TimestampAssigner):
    """
    Extracts the event-time timestamp from the Kafka message payload.
    Falls back to processing time if the field is missing or unparseable.
    """
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return int(json.loads(value).get("timestamp", int(time.time() * 1000)))
        except (json.JSONDecodeError, TypeError, ValueError):
            return int(time.time() * 1000)


# ─── Flink Functions ─────────────────────────────────────────────────────────

class ParseAndValidate(FlatMapFunction):
    """
    Parses JSON string into a dict.
    Valid records → output as JSON string.
    Invalid records → routed to dead-letter side output (logged here).
    """
    REQUIRED_FIELDS = {
        "machine_id", "timestamp", "gas_temperature",
        "gas_pressure", "humidity", "spin_rate", "torque"
    }

    def flat_map(self, value: str):
        try:
            record = json.loads(value)
            missing = self.REQUIRED_FIELDS - record.keys()
            if missing:
                log.warning(f"DLQ: missing fields {missing} in: {value[:120]}")
                return
            # Basic type coercion & range sanity check
            record["gas_temperature"] = float(record["gas_temperature"])
            record["gas_pressure"]    = float(record["gas_pressure"])
            record["humidity"]        = float(record["humidity"])
            record["spin_rate"]       = float(record["spin_rate"])
            record["torque"]          = float(record["torque"])
            record["timestamp"]       = int(record["timestamp"])
            yield json.dumps(record)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            log.warning(f"DLQ: parse error ({e}) for: {value[:120]}")


class DetectAnomalies(MapFunction):
    """
    Adds 'anomalies' list to each record flagging threshold breaches.
    Does not drop records — anomaly info enriches the record.
    """
    def map(self, value: str) -> str:
        record = json.loads(value)
        anomalies = []
        for field, limits in THRESHOLDS.items():
            v = record.get(field)
            if v is not None:
                if v < limits["min"]:
                    anomalies.append(f"{field}_LOW:{v}")
                elif v > limits["max"]:
                    anomalies.append(f"{field}_HIGH:{v}")
        record["anomalies"] = anomalies
        if anomalies:
            log.warning(f"ANOMALY [{record['machine_id']}]: {anomalies}")
        return json.dumps(record)


class WriteToIoTDB(MapFunction):
    """
    Writes each validated record to IoTDB.
    Opens a session on first call (lazy init — required in Flink distributed execution).
    """
    def __init__(self):
        self._session = None

    def _get_session(self) -> Session:
        if self._session is None:
            self._session = get_iotdb_session()
        return self._session

    def map(self, value: str) -> str:
        record = json.loads(value)
        try:
            write_record_to_iotdb(self._get_session(), record)
        except Exception as e:
            log.error(f"IoTDB write error for {record.get('machine_id')}: {e}")
            # Reset session so next call re-connects
            try:
                if self._session:
                    self._session.close()
            except Exception:
                pass
            self._session = None
        return value  # pass through for downstream operators


class TumblingWindowAggregation(ProcessWindowFunction):
    """
    1-minute tumbling window: computes per-machine aggregations.
    Emits one summary record per machine per window.
    """
    def __init__(self):
        self._session = None

    def _get_session(self) -> Session:
        if self._session is None:
            self._session = get_iotdb_session()
        return self._session

    def process(self, key, context, elements):
        records = [json.loads(e) for e in elements]
        if not records:
            return

        temps     = [r["gas_temperature"] for r in records]
        pressures = [r["gas_pressure"]    for r in records]
        spins     = [r["spin_rate"]        for r in records]
        torques   = [r["torque"]           for r in records]
        faults    = sum(1 for r in records if r.get("fault_code"))

        agg = {
            "machine_id":            key,
            "window_end_ts":         context.window().end,
            "reading_count":         len(records),
            "fault_count":           faults,
            "avg_gas_temperature":   round(sum(temps)     / len(temps),     2),
            "max_gas_temperature":   round(max(temps),     2),
            "min_gas_temperature":   round(min(temps),     2),
            "avg_gas_pressure":      round(sum(pressures) / len(pressures), 2),
            "max_gas_pressure":      round(max(pressures), 2),
            "avg_spin_rate":         round(sum(spins)     / len(spins),     2),
            "max_spin_rate":         round(max(spins),     2),
            "avg_torque":            round(sum(torques)   / len(torques),   2),
        }

        log.info(
            f"[WINDOW] {key} | {len(records)} readings | "
            f"avg_temp={agg['avg_gas_temperature']} | faults={faults}"
        )

        try:
            write_aggregation_to_iotdb(self._get_session(), agg)
        except Exception as e:
            log.error(f"IoTDB aggregation write error for {key}: {e}")
            try:
                if self._session:
                    self._session.close()
            except Exception:
                pass
            self._session = None

        yield json.dumps(agg)

# ─── Pipeline Definition ─────────────────────────────────────────────────────

def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Checkpointing — enables exactly-once semantics and crash recovery
    env.enable_checkpointing(60_000)  # checkpoint every 60 seconds
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.set_parallelism(2)  # 2 parallel task slots per TaskManager

    # ── Kafka Source ──────────────────────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # FIX: Watermark strategy now uses a named TimestampAssigner class
    # instead of an inline lambda.  See KafkaEventTimestampAssigner above.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(KafkaEventTimestampAssigner())
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="KafkaSource"
    )

    # ── Stage 1: Parse & Validate ─────────────────────────────────────────────
    valid_stream = raw_stream.flat_map(
        ParseAndValidate(),
        output_type=Types.STRING()
    ).name("ParseAndValidate")

    # ── Stage 2: Anomaly Detection ────────────────────────────────────────────
    enriched_stream = valid_stream.map(
        DetectAnomalies(),
        output_type=Types.STRING()
    ).name("DetectAnomalies")

    # ── Stage 3: Write raw records to IoTDB ──────────────────────────────────
    enriched_stream.map(
        WriteToIoTDB(),
        output_type=Types.STRING()
    ).name("WriteRawToIoTDB")

    # ── Stage 4: 1-minute windowed aggregations per machine ───────────────────
    (
        enriched_stream
        .key_by(lambda v: json.loads(v)["machine_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(TumblingWindowAggregation(), output_type=Types.STRING())
        .name("1MinWindowAggregation")
    )

    return env


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("Flink IoT Pipeline starting")
    log.info(f"  Kafka : {KAFKA_BOOTSTRAP_SERVERS} → topic: {KAFKA_TOPIC}")
    log.info(f"  IoTDB : {IOTDB_HOST}:{IOTDB_PORT} → {IOTDB_STORAGE_GROUP}")
    log.info("=" * 60)

    env = build_pipeline()
    env.execute("IoT Data Pipeline — Kafka → IoTDB")

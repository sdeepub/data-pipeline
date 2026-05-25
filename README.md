# 🏭 IoT Data Pipeline
## Kafka → Flink → IoTDB → Grafana

> **Status: Phase 0 complete — learning/demo kit, NOT production ready.**  
> Built and validated on Minikube (single-node Kubernetes). See roadmap below for the path to production.

**Stack:** Docker Compose · Kubernetes · Minikube · 100-Machine Simulation  
**Target:** Debian notebook, single-node learning environment  
**Problem domain:** On-premises factory monitoring → multi-site → cloud SaaS (see roadmap)

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Target Journey](#2-target-journey)
3. [Architecture](#3-architecture)
4. [Technology Stack](#4-technology-stack)
5. [Prerequisites — Debian Setup](#5-prerequisites--debian-setup)
6. [Quickstart — Docker Compose](#6-quickstart--docker-compose-start-here)
7. [Kubernetes Deployment (Minikube)](#7-kubernetes-deployment-minikube)
8. [Data Flow](#8-data-flow)
9. [Ports Quick Reference](#9-ports-quick-reference)
10. [Grafana Setup](#10-grafana-setup)
11. [Known Issues & Lessons Learned](#11-known-issues--lessons-learned)
12. [Roadmap](#12-roadmap)
13. [Production Gaps](#13-production-gaps)
14. [Contributing](#14-contributing)

---

## 1. Project Overview

This project builds a production-grade, real-time IoT data pipeline starting from a single Debian notebook — designed for engineers and tech owners who need to understand industrial data engineering end-to-end.

**Current capability (Phase 0):**
- Simulate 100 industrial machines streaming sensor telemetry
- Ingest data through Apache Kafka (fault-tolerant message bus)
- Process streams in real-time with Apache Flink (aggregations, anomaly detection)
- Persist time-series data in Apache IoTDB (purpose-built for sensor data)
- Visualise live dashboards in Grafana

**Problem domain this is moving toward:**  
Real on-premises factory monitoring — connecting to OPC-UA, Modbus PLC, SECS/GEM semiconductor equipment, and MQTT IIoT sensors, with contextual enrichment (lot-id, recipe, part-no, track-in/out) stored alongside telemetry in IoTDB as a single source of truth.

---

## 2. Target Journey

Each phase is a valid, useful stopping point. Later phases build on earlier ones without discarding prior work.

```
Phase 0 ✅  Learning / Demo Kit
            Simulator → Kafka → Flink → IoTDB → Grafana
            Local Docker Compose + Minikube
            100 virtual machines, pipeline proven end-to-end

Phase 1     On-Prem Factory MVP
            Real machines via edge layer (OPC-UA, Modbus, SECS/GEM, MQTT)
            Single factory, single tenant, hardened security
            PyFlink (simple operators, < 10k msg/sec)
            IoTDB as single source of truth (telemetry + contextual)

Phase 2     Multi-Site On-Prem Platform
            Edge-per-factory → centralized Kafka/Flink/IoTDB
            Java Flink (stateful joins, contextual enrichment, OEE, SPC)
            Contextual data (lot/recipe/track-in/out) co-located in IoTDB
            ETL to analytical layer (DuckDB/Postgres) for business reporting
            HA: 3-node Kafka, clustered Flink, replicated IoTDB

Phase 3     Cloud SaaS (stretch goal)
            Multi-tenant, cloud-native (EKS/GKE)
            Java Flink mandatory
            Commercial product — sell monitoring as a service
```

### PyFlink → Java Flink Migration Plan

PyFlink is appropriate for Phase 0 and Phase 1 (simple operators, moderate throughput). Java Flink becomes necessary at Phase 2 for:
- Stateful stream joins (telemetry + contextual events)
- High-throughput multi-site deployments
- Complex Event Processing (CEP) for anomaly patterns
- Production SLAs with predictable latency

The migration is **not a rewrite** — Flink's dataflow concepts (sources, operators, sinks, windows) are identical between Python and Java. Only syntax changes. The pipeline topology designed today survives intact.

```
Python stays at:   Edge layer (OPC-UA/Modbus/SECS/GEM adapters)
Java takes over:   Stream processing (Flink pipeline)
```

### Edge Layer (required for Phase 1)

Real factory protocols don't push JSON to Kafka natively. An edge layer is mandatory:

```
Factory Floor                  Edge Layer                Server Room
[OPC-UA Server]  ────────→  Node-RED          ──→  Kafka
[Modbus PLC]     ────────→  Protocol adapters ──→  (kafka-broker:29092)
[SECS/GEM Tool]  ────────→  Python/secsgem    ──→
[MQTT Broker]    ────────→  EMQX rules        ──→
[MES/ERP]        ────────→  Context adapter   ──→  IoTDB (contextual)
```

Recommended tools:
- **Node-RED** — OPC-UA + Modbus (connectors out of the box, visual, rapid)
- **Python + secsgem** — SECS/GEM (no good open-source edge tool handles GEM)
- **EMQX** — MQTT broker + rules engine if MQTT volume is high

### IoTDB as Single Source of Truth

Telemetry and contextual data are co-located in IoTDB on the same time axis:

```
root.factory1.MC001.gas_temperature   FLOAT   ← telemetry
root.factory1.MC001.gas_pressure      FLOAT   ← telemetry
root.factory1.MC001.spin_rate         FLOAT   ← telemetry
root.factory1.MC001.lot_id            TEXT    ← contextual
root.factory1.MC001.recipe_name       TEXT    ← contextual
root.factory1.MC001.status            TEXT    ← contextual
```

This enables natural time-correlated queries — "show me gas_temperature for lot ABC123 during recipe XYZ" — without joining across systems. ETL to a relational analytical layer (Postgres/DuckDB) is a Phase 2 addition driven by business reporting requirements, not a Phase 1 prerequisite.

---

## 3. Architecture

### Phase 0 (current)

```
[100 Machine Simulators]
         │ JSON telemetry (sensor-topic)
         ▼
   ┌─────────────┐
   │    Kafka    │  fault-tolerant message bus
   │  + Zookeeper│
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │    Flink    │  parse · validate · anomaly detect · 1-min aggregations
   │ JobManager  │
   │ TaskManager │
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │    IoTDB    │  time-series storage: root.factory1.MC001…100
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │   Grafana   │  live dashboards
   └─────────────┘
```

### Phase 1 (target — on-prem factory)

```
[OPC-UA / Modbus / SECS/GEM / MQTT]
         │
         ▼
   ┌─────────────┐
   │  Edge Layer │  Node-RED · Python/SECS · EMQX
   │  (per site) │  protocol translation → unified JSON
   └──────┬──────┘
          │ telemetry topic + contextual topic
          ▼
   ┌─────────────┐
   │    Kafka    │
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │    Flink    │  parse · enrich · detect · aggregate
   │  (PyFlink)  │
   └──────┬──────┘
          │
          ▼
   ┌─────────────────────────────┐
   │           IoTDB             │  telemetry + contextual (single source of truth)
   │  root.factory1.MC001.*      │
   │    gas_temperature  FLOAT   │
   │    lot_id           TEXT    │
   │    recipe_name      TEXT    │
   └──────┬──────────────────────┘
          │
          ▼
   ┌─────────────┐
   │   Grafana   │
   └─────────────┘
```

---

## 4. Technology Stack

| Component | Image / Version | Role | Port(s) |
|---|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.5.0 | Kafka coordination | 2181 |
| Apache Kafka | confluentinc/cp-kafka:7.5.0 | Message broker | 9092, 29092 |
| Apache Flink | flink-pipeline:latest (custom) | Stream processing | 8081 (UI), 6123 (RPC), 6124 (BLOB) |
| Apache IoTDB | apache/iotdb:1.3.0-standalone | Time-series database | 6667 (Thrift), 18080 (REST) |
| Grafana | grafana/grafana:10.2.2 | Visualisation | 3000 |
| Simulator | machine-simulator:latest (custom) | 100 virtual machines | — |

> **Note:** The custom Flink image (`flink-pipeline:latest`) is built from `flink/Dockerfile` and includes PyFlink, the IoTDB Python SDK, Kafka connector JARs, and `pipeline_job.py`. The vanilla `flink:1.17` upstream image will not work.

---

## 5. Prerequisites — Debian Setup

### System Requirements

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 16 GB |
| Disk | 20 GB free | 40 GB free |
| OS | Debian 11 (Bullseye) | Debian 12 (Bookworm) |

### Install Docker

```bash
sudo apt-get update && sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/debian $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER && newgrp docker
```

### Install Minikube & kubectl

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

minikube start --driver=docker --cpus=4 --memory=8192 --disk-size=30g
```

---

## 6. Quickstart — Docker Compose (Start Here)

> ⚠️ Do Docker Compose before Kubernetes. Much easier to debug, and you understand each service before adding orchestration complexity.

```bash
git clone https://github.com/sdeepub/data-pipeline.git
cd data-pipeline
docker compose up -d
docker compose ps          # all should show healthy
```

Watch data flowing:
```bash
docker compose logs -f simulator
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-topic --from-beginning
```

Scale to 100 machines:
```bash
docker compose up -d --scale simulator=10   # 10 replicas × 10 machines
```

Stop / clean up:
```bash
docker compose down       # stop, keep volumes
docker compose down -v    # stop and delete all data
```

---

## 7. Kubernetes Deployment (Minikube)

### 7.1 Start Minikube

```bash
minikube start --driver=docker --cpus=4 --memory=8192 --disk-size=30g
```

> If Minikube already exists with different resources, delete and recreate:
> `minikube delete && minikube start --driver=docker --cpus=4 --memory=8192 --disk-size=30g`

### 7.2 One-Command Deploy

```bash
./scripts/deploy.sh
```

This script:
1. Builds `machine-simulator:latest` and `flink-pipeline:latest` images
2. Loads both into Minikube (no registry push needed)
3. Applies all manifests in dependency order
4. Submits the Flink pipeline job
5. Prints access URLs

### 7.3 Manual Deploy (step by step)

```bash
# Build and load images
docker build -t machine-simulator:latest ./simulators
docker build -t flink-pipeline:latest ./flink
minikube image load machine-simulator:latest
minikube image load flink-pipeline:latest

# Apply in order — each waits for the previous to be Ready
kubectl apply -f k8s/namespace/namespace.yaml
kubectl apply -f k8s/zookeeper/zookeeper.yaml
kubectl rollout status deployment/zookeeper -n data-pipeline --timeout=120s

kubectl apply -f k8s/kafka/kafka.yaml
kubectl rollout status deployment/kafka -n data-pipeline --timeout=120s

kubectl apply -f k8s/iotdb/iotdb.yaml
kubectl rollout status deployment/iotdb -n data-pipeline --timeout=180s

kubectl apply -f k8s/flink/flink.yaml
kubectl rollout status deployment/flink-jobmanager -n data-pipeline --timeout=180s
kubectl rollout status deployment/flink-taskmanager -n data-pipeline --timeout=180s

# Submit the pipeline job (must be after Flink cluster is ready)
kubectl apply -f k8s/flink/flink-job.yaml

kubectl apply -f k8s/grafana/grafana.yaml
kubectl apply -f k8s/simulators/simulator.yaml
```

### 7.4 Verify Pipeline is Running

```bash
# All pods should be 1/1 Running
kubectl get pods -n data-pipeline

# Flink job should show RUNNING (not RESTARTING)
curl -s http://$(minikube ip):30081/v1/jobs | python3 -m json.tool

# Simulators sending data
kubectl logs -n data-pipeline -l app=simulator --tail=10

# IoTDB receiving data
POD=$(kubectl get pod -n data-pipeline -l app=iotdb -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n data-pipeline $POD -- \
  /iotdb/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root \
  -e "SELECT COUNT(*) FROM root.factory1.**;"
```

### 7.5 Pause / Resume (saves resources)

```bash
minikube pause    # freeze cluster, keep all state
minikube unpause  # resume exactly where you left off
```

After unpause, verify Flink job is still RUNNING — if RESTARTING, restart the cluster components:
```bash
kubectl rollout restart deployment/flink-jobmanager -n data-pipeline
kubectl rollout restart deployment/flink-taskmanager -n data-pipeline
# Then resubmit the job
kubectl delete job flink-job-submit -n data-pipeline
kubectl apply -f k8s/flink/flink-job.yaml
```

### 7.6 Teardown

```bash
./scripts/deploy.sh teardown
# or
kubectl delete namespace data-pipeline
```

---

## 8. Data Flow

### Simulator → Kafka

Each virtual machine publishes JSON every 2 seconds to topic `sensor-topic`:

```json
{
  "machine_id": "MC042",
  "machine_type": "compressor",
  "location": "zone_B",
  "timestamp": 1715123456789,
  "gas_temperature": 423.5,
  "gas_pressure": 5.2,
  "humidity": 45.1,
  "spin_rate": 2850.0,
  "torque": 12.3,
  "status": "running",
  "fault_code": null
}
```

Fault injection: 2% probability per reading triggers F001–F004 profiles (overtemp, overpressure, high vibration, low humidity).

### Kafka → Flink

Four processing stages:

1. **ParseAndValidate** — JSON parsing, field validation, type coercion. Malformed messages logged as DLQ
2. **DetectAnomalies** — threshold checking per sensor, anomaly list added to record
3. **WriteRawToIoTDB** — every valid record written to `root.factory1.<machine_id>`
4. **1MinWindowAggregation** — tumbling 1-minute windows per machine: avg/max/min per sensor, fault count, written to `root.factory1_agg.<machine_id>`

### Flink → IoTDB

```
root.factory1
├── MC001
│   ├── gas_temperature   FLOAT (GORILLA encoding)
│   ├── gas_pressure      FLOAT
│   ├── humidity          FLOAT
│   ├── spin_rate         FLOAT
│   └── torque            FLOAT
├── MC002 … MC030
│
root.factory1_agg
├── MC001
│   ├── avg_gas_temperature
│   ├── max_gas_temperature
│   ├── fault_count       INT32
│   └── reading_count     INT32
```

---

## 9. Ports Quick Reference

| Service | Port | Access | Notes |
|---|---|---|---|
| Zookeeper | 2181 | Internal only | — |
| Kafka (internal) | 29092 | `kafka-broker:29092` | Pod-to-pod |
| Kafka (external) | 9092 / NodePort 30092 | Host only | — |
| Flink Web UI | 8081 / NodePort 30081 | `http://<minikube-ip>:30081` | — |
| Flink RPC | 6123 | Internal only | — |
| Flink BLOB | 6124 | Internal only | Fixed — required for task distribution |
| IoTDB Thrift | 6667 / NodePort 30667 | CLI/SDK | — |
| IoTDB REST | 18080 | Internal only | Grafana datasource |
| Grafana | 3000 / NodePort 30300 | `http://<minikube-ip>:30300` | admin/admin |

> **Important:** The Kafka internal service name is `kafka-broker` (not `kafka`). All internal services must use `kafka-broker:29092`.

---

## 10. Grafana Setup

### Access
```
http://<minikube-ip>:30300   # Kubernetes
http://localhost:3000         # Docker Compose
admin / admin
```

### Add IoTDB Datasource (manual)
1. Connections → Data Sources → Add data source → Apache IoTDB
2. URL: `http://iotdb:18080`
3. Username: `root` | Password: `root`
4. Save & Test → should show green

> **Note:** Auto-provisioning via ConfigMap is configured but may not load on first boot due to plugin download timing. Add manually if the datasource is not pre-configured.

### Sample Queries
```sql
-- Latest readings all machines
SELECT * FROM root.factory1.** WHERE time > now() - 5m

-- Average temperature per machine last hour
SELECT AVG(gas_temperature) FROM root.factory1.**
WHERE time > now() - 1h GROUP BY ([now()-1h, now()), 1m)

-- Fault events
SELECT * FROM root.factory1.**
WHERE gas_temperature > 460 OR gas_pressure > 6.5
```

---

## 11. Known Issues & Lessons Learned

These were discovered and fixed during the initial Minikube deployment. Documented here so future contributors don't repeat the debugging.

| Issue | Root Cause | Fix Applied |
|---|---|---|
| Flink pods had no PyFlink/IoTDB | K8s manifest used upstream `flink:1.17` image, not custom Dockerfile | Build `flink-pipeline:latest` from `flink/Dockerfile`, load into Minikube |
| No job submission on K8s | No K8s Job equivalent of docker-compose `flink-job` service | Added `k8s/flink/flink-job.yaml` (Kubernetes Job kind) |
| Kafka DNS not resolving | Service named `kafka-broker`, not `kafka` | Fixed all references to `kafka-broker:29092` |
| IoTDB REST probe failing | Default REST port is `18080` not `8080`; `/api/v1/ping` path doesn't exist in 1.3.0 | Port fixed to `18080`; probe switched to TCP check on port `6667` |
| IoTDB liveness probe killing pod | IoTDB startup is slow (~60s); liveness probe killed it before REST bound | Removed liveness probe; readiness probe on Thrift port `6667` |
| Flink BLOB transfer failing | BLOB server uses random port, not exposed in service | Fixed `blob.server.port: 6124` in ConfigMap; added port to service |
| Flink job RESTARTING after reboot | TaskManagers cached stale JobManager IP after pod restart | `kubectl rollout restart` both deployments; resubmit job |
| PyFlink lambda not serializable | Inline lambda cannot be pickled for distributed execution | Replaced with `KafkaEventTimestampAssigner(TimestampAssigner)` named class |
| Grafana wrong plugin | K8s manifest had `alexanderzobnin-zabbix-app` instead of `apache-iotdb-datasource` | Fixed plugin name in env var |
| Simulator restartPolicy invalid | `restartPolicy: Always` inside `containers[]` is not valid K8s | Removed — Deployments restart pods automatically |
| IoTDB env vars wrong | `cn_*` vars are for ConfigNode/cluster mode, not standalone | Replaced with `enable_rest_service=true` and `dn_*` vars |

---

## 12. Roadmap

### Phase 0 ✅ — Learning / Demo Kit
**Done.** Single-node Minikube, 100 virtual machines, full pipeline proven.

### Phase 1 — On-Prem Factory MVP
**Goal:** Connect to real factory equipment in a single facility.

- [ ] Edge layer: Node-RED OPC-UA + Modbus adapters
- [ ] Edge layer: Python SECS/GEM adapter (secsgem library)
- [ ] Edge layer: EMQX MQTT broker integration
- [ ] Contextual Kafka topic: lot-id, recipe, track-in/out events
- [ ] Flink: consume contextual topic, write to IoTDB as TEXT timeseries
- [ ] IoTDB schema: add TEXT fields for contextual data alongside telemetry
- [ ] Security: TLS/SASL for Kafka, auth for IoTDB and Grafana
- [ ] Secrets management: k8s Secrets + RBAC
- [ ] Grafana dashboards: live telemetry, fault rate, anomaly heatmap
- [ ] Grafana: lot/recipe correlation panels
- [ ] Readiness/liveness probes and resource limits hardened
- [ ] Basic runbooks: startup, shutdown, backup, restore

**Flink:** PyFlink (simple operators, contextual write)  
**Storage:** IoTDB as single source of truth (telemetry + contextual)  
**Estimated effort:** 8–12 weeks (2 engineers)

### Phase 2 — Multi-Site On-Prem Platform
**Goal:** Multiple factories, centralized monitoring, business reporting.

- [ ] Edge-per-factory → centralized Kafka (hub-and-spoke)
- [ ] Migrate Flink pipeline to Java (stateful joins, OEE, SPC windows)
- [ ] Flink: stream join telemetry + contextual events
- [ ] ETL pipeline: IoTDB → DuckDB/Postgres for business reporting
- [ ] BI layer: OEE calculations, yield correlation, SPC charts
- [ ] HA: 3-node Kafka cluster, Flink JobManager HA, replicated IoTDB
- [ ] Automated backups and tested restore procedures
- [ ] Centralized logging, metrics exporters, Grafana alerting
- [ ] CI/CD: GitHub Actions, image builds, integration tests
- [ ] Helm charts for repeatable multi-site deployment

**Flink:** Java (mandatory for stateful joins and production SLAs)  
**Storage:** IoTDB (primary) + DuckDB/Postgres (analytical layer)  
**Estimated effort:** 16–24 weeks (2–3 engineers)

### Phase 3 — Cloud SaaS (stretch goal)
**Goal:** Multi-tenant commercial product — sell factory monitoring as a service.

- [ ] Multi-tenant architecture: per-tenant Kafka topics, IoTDB namespaces
- [ ] Cloud-native deployment: EKS/GKE, managed Kafka (MSK/Confluent)
- [ ] OAuth2/OIDC authentication, per-tenant RBAC
- [ ] Tenant onboarding automation
- [ ] SLA monitoring and per-tenant observability
- [ ] Billing integration
- [ ] Security audit, penetration testing, compliance (ISO 27001, SOC 2)
- [ ] Global availability, multi-region IoTDB

**Flink:** Java mandatory  
**Estimated effort:** 6–12 months (4–6 engineers)

### Prioritized Effort Estimates

| Task | Phase | Effort (days) |
|---|---|---|
| Node-RED OPC-UA + Modbus adapter | 1 | 3–5 |
| Python SECS/GEM adapter | 1 | 5–8 |
| Contextual Kafka topic + Flink writer | 1 | 3–5 |
| IoTDB contextual schema + Grafana panels | 1 | 2–3 |
| TLS/SASL Kafka + IoTDB/Grafana auth | 1 | 3–5 |
| Grafana dashboards (telemetry + context) | 1 | 3–5 |
| Java Flink migration | 2 | 8–12 |
| Flink stateful stream join | 2 | 5–8 |
| ETL IoTDB → DuckDB/Postgres | 2 | 4–6 |
| 3-node Kafka HA | 2 | 4–6 |
| Flink JobManager HA | 2 | 4–6 |
| CI/CD + GitHub Actions | 2 | 4–7 |
| Helm charts | 2 | 5–8 |
| Multi-tenant architecture | 3 | 15–20 |
| Cloud-native deployment | 3 | 10–15 |

---

## 13. Production Gaps

This repository is intentionally a learning/demo starter kit. The following gaps must be addressed before any production or internet-facing deployment:

**Security**
- Plaintext Kafka (no TLS/SASL)
- No authentication on IoTDB or Grafana beyond basic password
- Secrets stored as plaintext env vars (not Vault or encrypted Secrets)
- No network policies between pods

**Reliability**
- Single-node everything (no HA for Kafka, Flink, or IoTDB)
- No automated backups or tested restore procedures
- Flink checkpoints lost on pod restart (ephemeral PVC on single-node Minikube)

**Observability**
- No centralized logging
- No metrics exporters (Prometheus/Grafana alerting not configured)
- No SLO dashboards or on-call runbooks

**Operations**
- No CI/CD pipeline
- No automated tests (unit, integration, chaos)
- Component versions not pinned to a tested compatibility matrix
- No Helm charts for repeatable deployment

**Flink packaging**
- Pipeline submitted as raw Python script — not packaged as a proper artifact
- PyFlink version pinned to 1.17.2 — must match Flink cluster version exactly

---

## 14. Contributing

PRs welcome. Suggested first contributions:
- GitHub Actions: lint, unit tests, Docker image build and smoke test
- Helm chart for Minikube and on-prem deployment
- Node-RED OPC-UA adapter flow
- Grafana dashboard JSON (telemetry + anomaly panels)
- Java Flink port of `pipeline_job.py`
- Improved Grafana auto-provisioning (fix plugin timing issue)

Please open an issue first for major changes.

## License

MIT

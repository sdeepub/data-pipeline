# 🏭 IoT Data Pipeline
## Kafka → Flink → IoTDB → Grafana
*A complete learning guide for tech owners & system architects*

**Stack:** Docker Compose · Kubernetes · Minikube · 100-Machine Simulation  
**Target:** Debian notebook, single-node learning environment

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Technology Stack](#2-technology-stack)
3. [Prerequisites — Debian Setup](#3-prerequisites--debian-notebook-setup)
4. [Repository Structure](#4-repository-structure)
5. [Quickstart — Docker Compose](#5-quickstart--docker-compose-start-here)
6. [Data Flow — Step by Step](#6-data-flow--step-by-step)
7. [Kubernetes Deployment (Minikube)](#7-kubernetes-deployment-minikube)
8. [Ports Quick Reference](#8-ports-quick-reference)
9. [Grafana Dashboard Setup](#9-grafana-dashboard-setup)
10. [Troubleshooting](#10-troubleshooting)
11. [Recommended Learning Path](#11-recommended-learning-path)

---

## 1. Project Overview

This project builds a production-grade, real-time IoT data pipeline on a single Debian notebook — designed for tech owners who need to understand data engineering end-to-end.

You will:
- Simulate **100 industrial machines** streaming sensor telemetry
- Ingest data through **Apache Kafka** (fault-tolerant message bus)
- Process streams in real-time with **Apache Flink** (aggregations, anomaly detection)
- Persist time-series data in **Apache IoTDB** (purpose-built for sensor data)
- Visualise live dashboards in **Grafana**

### Architecture

```
[100 Machine Simulators]
         │
         ▼  (JSON over Kafka topic: machine-telemetry)
   ┌─────────────┐
   │    Kafka    │  ← fault-tolerant, durable message bus
   │  + Zookeeper│
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │    Flink    │  ← stream processing: parse, aggregate, detect anomalies
   │ JobManager  │
   │ TaskManager │
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │    IoTDB    │  ← time-series DB: root.factory1.machine_001…100
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │   Grafana   │  ← live dashboards, alerts
   └─────────────┘
```

---

## 2. Technology Stack

| Component | Image / Version | Role | Port(s) |
|---|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.5.0 | Kafka coordination | 2181 |
| Apache Kafka | confluentinc/cp-kafka:7.5.0 | Message broker | 9092, 29092 |
| Apache Flink | flink:1.17-scala_2.12-java11 | Stream processing | 8081 (UI), 6123 |
| Apache IoTDB | apache/iotdb:latest | Time-series database | 6667, 8080 |
| Grafana | grafana/grafana:10.2.2 | Visualisation | 3000 |
| Simulator | custom Python (./simulators) | 100 virtual machines | — |

---

## 3. Prerequisites — Debian Notebook Setup

### System Requirements

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 16 GB |
| Disk | 20 GB free | 40 GB free |
| OS | Debian 11 (Bullseye) | Debian 12 (Bookworm) |

### Install Docker & Docker Compose

```bash
# 1. Update and install dependencies
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# 2. Add Docker's GPG key and repo
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/debian $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list

# 3. Install Docker Engine
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# 4. Add your user to docker group (no sudo needed)
sudo usermod -aG docker $USER && newgrp docker

# 5. Verify
docker --version && docker compose version
```

### Install Minikube & kubectl (for Kubernetes deployment)

```bash
# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

# Start Minikube (Docker driver is best on Debian)
minikube start --driver=docker --cpus=4 --memory=8192 --disk-size=30g
```

> 💡 **Tip:** Run `minikube dashboard` to open the Kubernetes web UI — excellent for understanding what's running inside the cluster.

---

## 4. Repository Structure

```
data-pipeline/
├── docker-compose.yml          # Full local stack — single command startup
├── .gitignore
├── README.md
│
├── simulators/                 # IoT machine simulator
│   ├── Dockerfile
│   ├── simulator.py            # 100 virtual machines, 1 msg/sec each
│   └── requirements.txt
│
├── flink/                      # Flink stream processing jobs
│   ├── Dockerfile
│   ├── pipeline_job.py         # Kafka consumer → IoTDB writer
│   └── requirements.txt
│
├── k8s/                        # Kubernetes manifests
│   ├── namespace.yaml
│   ├── zookeeper.yaml
│   ├── kafka.yaml
│   ├── iotdb.yaml
│   ├── flink/
│   │   ├── jobmanager.yaml
│   │   └── taskmanager.yaml
│   ├── grafana.yaml
│   └── simulator.yaml
│
├── scripts/
│   ├── setup.sh                # One-command environment setup
│   ├── start.sh
│   ├── stop.sh
│   └── reset.sh                # Wipe data and restart clean
│
└── docs/
    ├── grafana-dashboard.json  # Pre-built dashboard — import this
    └── architecture.png
```

---

## 5. Quickstart — Docker Compose (Start Here)

> ⚠️ **Do this before Kubernetes.** Docker Compose is much easier to debug and lets you understand each service before adding orchestration complexity.

### Step 1 — Clone the repo

```bash
git clone https://github.com/sdeepub/data-pipeline.git
cd data-pipeline
```

### Step 2 — Start the full stack

```bash
docker compose up -d
```

First run pulls ~2–3 GB of images and may take 5–10 minutes. Subsequent starts take ~30 seconds.

### Step 3 — Verify all services are healthy

```bash
docker compose ps
```

All containers should show `healthy`. Check each service:

```bash
# Zookeeper
echo ruok | nc localhost 2181                         # → "imok"

# Kafka — list topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Flink UI
open http://localhost:8081

# Grafana
open http://localhost:3000                            # admin / admin

# IoTDB CLI
docker exec -it iotdb /iotdb/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
```

### Step 4 — Watch data flowing

```bash
# Simulator logs
docker compose logs -f simulator

# Kafka messages arriving live
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic machine-telemetry \
  --from-beginning
```

### Step 5 — Scale to 100 machines

The compose file starts 3 simulator replicas (~30 machines). Scale up:

```bash
docker compose up -d --scale simulator=10    # 10 replicas × 10 machines = 100
```

> 💡 Each simulator replica manages ~10 virtual machines independently.

### Stop / Clean Up

```bash
docker compose down          # Stop, keep data volumes
docker compose down -v       # Stop AND delete all data (fresh start)
```

---

## 6. Data Flow — Step by Step

### 6.1 Simulator → Kafka

Each virtual machine publishes a JSON message every second to topic `machine-telemetry`:

```json
{
  "machine_id": "machine_042",
  "timestamp": 1715123456789,
  "temperature": 72.3,
  "vibration": 0.42,
  "pressure": 101.8,
  "rpm": 3420,
  "status": "running",
  "fault_code": null
}
```

Kafka stores these durably. If Flink goes down, **no data is lost** — Flink picks up exactly where it left off on restart (exactly-once semantics).

### 6.2 Kafka → Flink (Stream Processing)

The Flink job consumes from Kafka and:

1. **Parses & validates** — malformed messages go to a dead-letter topic
2. **Windowed aggregations** — 1-minute tumbling windows compute min/max/avg per machine
3. **Anomaly detection** — flags readings outside configured thresholds (e.g. temp > 90°C)
4. **Enrichment** — joins machine metadata (location, machine type, operator)

### 6.3 Flink → IoTDB

Processed records write to IoTDB organised as:

```
root.factory1
├── machine_001
│   ├── temperature   (FLOAT)
│   ├── vibration     (FLOAT)
│   ├── pressure      (FLOAT)
│   ├── rpm           (INT32)
│   └── status        (TEXT)
├── machine_002
│   └── ...
...
└── machine_100
```

### 6.4 IoTDB → Grafana

Grafana queries IoTDB with IoTQL:

```sql
SELECT AVG(temperature), MAX(vibration)
FROM root.factory1.**
WHERE time > now() - 5m
GROUP BY ([now()-1h, now()), 1m)
```

---

## 7. Kubernetes Deployment (Minikube)

Once comfortable with Docker Compose, deploy to Kubernetes for a realistic production experience.

### 7.1 Enable Minikube Addons

```bash
minikube addons enable ingress
minikube addons enable metrics-server
minikube addons enable storage-provisioner
```

### 7.2 Deploy the Pipeline

```bash
# Namespace first
kubectl apply -f k8s/namespace.yaml

# Infrastructure (in order — wait for each to be Ready before the next)
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/iotdb.yaml

# Flink
kubectl apply -f k8s/flink/jobmanager.yaml
kubectl apply -f k8s/flink/taskmanager.yaml

# Application layer
kubectl apply -f k8s/grafana.yaml
kubectl apply -f k8s/simulator.yaml
```

### 7.3 Monitor the Deployment

```bash
# Watch pods come up
kubectl get pods -n data-pipeline -w

# Logs for all simulators
kubectl logs -n data-pipeline -l app=simulator --tail=50 -f

# Access Flink UI
kubectl port-forward -n data-pipeline svc/flink-jobmanager 8081:8081

# Access Grafana
kubectl port-forward -n data-pipeline svc/grafana 3000:3000
```

### 7.4 Scale Simulators

```bash
kubectl scale deployment simulator -n data-pipeline --replicas=10
```

> ⚠️ On a laptop, start with 3–5 replicas and scale gradually. Monitor resources with:
> `kubectl top pods -n data-pipeline`

---

## 8. Ports Quick Reference

| Service | Port | Access | Credentials |
|---|---|---|---|
| Zookeeper | 2181 | Internal only | — |
| Kafka (external) | 9092 | localhost:9092 | — |
| Kafka (inter-container) | 29092 | kafka:29092 | — |
| Flink Web UI | 8081 | http://localhost:8081 | — |
| Flink RPC | 6123 | Internal only | — |
| IoTDB CLI/JDBC | 6667 | localhost:6667 | root / root |
| IoTDB REST API | 8080 | http://localhost:8080 | root / root |
| Grafana | 3000 | http://localhost:3000 | admin / admin |

---

## 9. Grafana Dashboard Setup

### Add IoTDB as a Data Source

1. Open **http://localhost:3000** → log in (admin / admin)
2. Go to **Connections → Data Sources → Add data source**
3. Search for **Apache IoTDB** and select it
4. URL: `http://iotdb:8080`
5. Username: `root` | Password: `root`
6. Click **Save & Test** → you should see *"Data source is working"*

### Import the Pre-built Dashboard

1. Go to **Dashboards → Import**
2. Upload `docs/grafana-dashboard.json`
3. Select your IoTDB data source
4. Click **Import**

The dashboard includes: live machine count, temperature heatmap, vibration over time, fault rate, and per-machine drill-down.

---

## 10. Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Kafka keeps restarting | Zookeeper not ready yet | Wait for Zookeeper `healthy`, then `docker compose restart kafka` |
| Simulator: "Connection refused" | Kafka still starting | Simulators auto-retry; wait 30–60s after stack start |
| Flink job not running | Job not submitted | Open http://localhost:8081 and submit the job JAR |
| IoTDB REST ping fails | Startup takes ~30s | `docker compose logs iotdb` — wait for *"IoTDB is set up"* |
| Grafana data source error | Wrong URL | Use `http://iotdb:8080` (not `localhost`) inside Docker |
| Minikube OOM | RAM too low | `minikube stop && minikube start --memory=12288` |
| Pods stuck in Pending | Insufficient resources | `kubectl describe pod <name>` to see the constraint |

### Key Debug Commands

```bash
# All logs
docker compose logs -f --tail=100

# Kafka — list topics and consumer group lag
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --all-groups

# IoTDB — verify data is arriving
docker exec -it iotdb /iotdb/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
IoTDB> SHOW STORAGE GROUP;
IoTDB> SHOW TIMESERIES root.factory1.*;
IoTDB> SELECT * FROM root.factory1.machine_001 LIMIT 10;

# Flink — check job status via REST
curl http://localhost:8081/v1/jobs | python3 -m json.tool
```

---

## 11. Recommended Learning Path

Follow this sequence as a tech owner learning the stack for the first time:

### Week 1 — Foundation
1. Get Docker Compose stack running (`docker compose up -d`)
2. Explore each service UI (Flink at :8081, Grafana at :3000, IoTDB CLI)
3. Read simulator logs and understand the JSON message structure
4. Use `kafka-console-consumer` to watch live messages

### Week 2 — Understanding the Components
1. Modify the simulator to add a new sensor metric (e.g. `power_watts`)
2. Update the Flink job to pass through the new field
3. Add the measurement to the IoTDB schema
4. Create a new Grafana panel for the new metric

### Week 3 — Kubernetes
1. Deploy the stack to Minikube
2. Practice `kubectl get`, `logs`, `describe`, `scale`
3. Simulate a pod failure and watch Kubernetes restart it:
   `kubectl delete pod <flink-taskmanager-xxx>`
4. Scale simulators from 3 → 10 replicas and observe Grafana dashboards

### Week 4 — Production Thinking
1. Add Kafka retention and compaction policies
2. Configure Flink checkpointing (fault tolerance)
3. Set up Grafana alerting (email on temperature > threshold)
4. Document your own architecture decisions

---

## Contributing

Pull requests welcome. Please open an issue first for major changes.

## License

MIT

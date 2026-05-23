#!/usr/bin/env bash
# =============================================================
# deploy.sh  —  Deploy data-pipeline stack to Minikube
#
# Usage:
#   ./scripts/deploy.sh          # full deploy
#   ./scripts/deploy.sh teardown # delete everything
#
# What changed from the original:
#   - Builds and loads the custom Flink image (flink-pipeline)
#     so the K8s pods get PyFlink + IoTDB SDK + Kafka JARs
#   - Builds and loads the custom Grafana image
#     (apache-iotdb-datasource plugin pre-installed)
#   - Applies flink-job.yaml after the Flink cluster is ready
#     so pipeline_job.py is actually submitted
#   - Verifies the Flink job is running before declaring success
# =============================================================

set -euo pipefail

NAMESPACE="data-pipeline"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
K8S_DIR="$REPO_ROOT/k8s"
SIMULATOR_DIR="$REPO_ROOT/simulators"
FLINK_DIR="$REPO_ROOT/flink"
GRAFANA_DIR="$REPO_ROOT/grafana"

# ── helpers ───────────────────────────────────────────────────
info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m    $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
err()   { echo -e "\033[1;31m[ERR]\033[0m   $*" >&2; exit 1; }

wait_for_deploy() {
  local name=$1
  info "Waiting for deployment/$name to be ready..."
  kubectl rollout status deployment/"$name" -n "$NAMESPACE" --timeout=180s \
    && ok "$name is ready" \
    || warn "$name timed out — check: kubectl get pods -n $NAMESPACE"
}

wait_for_job() {
  local name=$1
  local attempts=0
  info "Waiting for job/$name to complete..."
  while [[ $attempts -lt 30 ]]; do
    local status
    status=$(kubectl get job "$name" -n "$NAMESPACE" \
      -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
    local failed
    failed=$(kubectl get job "$name" -n "$NAMESPACE" \
      -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
    if [[ "$status" == "True" ]]; then
      ok "Job $name completed successfully"
      return 0
    fi
    if [[ "$failed" == "True" ]]; then
      warn "Job $name failed — check: kubectl logs -n $NAMESPACE job/$name"
      return 1
    fi
    sleep 10
    ((attempts++))
  done
  warn "Job $name timed out waiting for completion"
}

# ── teardown ──────────────────────────────────────────────────
if [[ "${1:-}" == "teardown" ]]; then
  info "Deleting namespace $NAMESPACE (all resources inside will be removed)..."
  kubectl delete namespace "$NAMESPACE" --ignore-not-found
  ok "Teardown complete"
  exit 0
fi

# ── pre-flight checks ─────────────────────────────────────────
command -v minikube >/dev/null || err "minikube not found in PATH"
command -v kubectl  >/dev/null || err "kubectl not found in PATH"
command -v docker   >/dev/null || err "docker not found in PATH"

if ! minikube status | grep -q "Running"; then
  warn "Minikube doesn't appear to be running. Starting it..."
  minikube start --memory=8192 --cpus=4 --driver=docker
fi

# ── build & load custom images ────────────────────────────────

# Simulator
info "Building machine-simulator image..."
docker build -t machine-simulator:latest "$SIMULATOR_DIR"
info "Loading machine-simulator into Minikube..."
minikube image load machine-simulator:latest
ok "machine-simulator image loaded"

# Flink (custom: PyFlink + IoTDB SDK + Kafka JARs + pipeline_job.py)
info "Building flink-pipeline image (this takes a few minutes on first run)..."
docker build -t flink-pipeline:latest "$FLINK_DIR"
info "Loading flink-pipeline into Minikube..."
minikube image load flink-pipeline:latest
ok "flink-pipeline image loaded"

# ── apply manifests (ordered) ─────────────────────────────────
info "Creating namespace..."
kubectl apply -f "$K8S_DIR/namespace/namespace.yaml"

info "Deploying Zookeeper..."
kubectl apply -f "$K8S_DIR/zookeeper/zookeeper.yaml"
wait_for_deploy zookeeper

info "Deploying Kafka..."
kubectl apply -f "$K8S_DIR/kafka/kafka.yaml"
wait_for_deploy kafka

info "Deploying IoTDB..."
kubectl apply -f "$K8S_DIR/iotdb/iotdb.yaml"
wait_for_deploy iotdb

info "Deploying Flink cluster..."
kubectl apply -f "$K8S_DIR/flink/flink.yaml"
wait_for_deploy flink-jobmanager
wait_for_deploy flink-taskmanager

# ── submit the Flink pipeline job ─────────────────────────────
info "Submitting Flink pipeline job..."
# Delete a previous submission if re-running (idempotent)
kubectl delete job flink-job-submit -n "$NAMESPACE" --ignore-not-found
kubectl apply -f "$K8S_DIR/flink/flink-job.yaml"
wait_for_job flink-job-submit

info "Deploying Grafana..."
kubectl apply -f "$K8S_DIR/grafana/grafana.yaml"
wait_for_deploy grafana

info "Deploying Simulator..."
kubectl apply -f "$K8S_DIR/simulators/simulator.yaml"
wait_for_deploy simulator

# ── print access URLs ─────────────────────────────────────────
MINIKUBE_IP=$(minikube ip)

echo ""
echo "======================================================"
ok "All components deployed and pipeline is running!"
echo ""
echo "  Service          URL"
echo "  ──────────────── ────────────────────────────────"
echo "  Grafana          http://$MINIKUBE_IP:30300  (admin/admin)"
echo "  Flink UI         http://$MINIKUBE_IP:30081"
echo "  IoTDB Thrift     $MINIKUBE_IP:30667"
echo ""
echo "  Or use port-forward:"
echo "    kubectl port-forward svc/grafana 3000:3000 -n $NAMESPACE"
echo "    kubectl port-forward svc/flink-jobmanager 8081:8081 -n $NAMESPACE"
echo "    kubectl port-forward svc/kafka 9092:9092 -n $NAMESPACE"
echo ""
echo "  Verify the pipeline is processing data:"
echo "    kubectl logs -n $NAMESPACE job/flink-job-submit"
echo "    curl http://$MINIKUBE_IP:30081/v1/jobs | python3 -m json.tool"
echo ""
echo "  Scale simulators:"
echo "    kubectl scale deploy/simulator --replicas=10 -n $NAMESPACE"
echo ""
echo "  Useful debug commands:"
echo "    kubectl get pods -n $NAMESPACE"
echo "    kubectl logs -l app=flink -n $NAMESPACE --tail=50"
echo "    kubectl logs -l app=simulator -n $NAMESPACE --tail=20"
echo "    kubectl logs -l app=iotdb -n $NAMESPACE --tail=30"
echo "======================================================"

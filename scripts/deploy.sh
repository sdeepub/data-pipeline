#!/usr/bin/env bash
# =============================================================
# deploy.sh — Deploy data-pipeline stack to Minikube
# Usage:
#   ./scripts/deploy.sh          # full deploy
#   ./scripts/deploy.sh teardown # delete everything
# =============================================================

set -euo pipefail

NAMESPACE="data-pipeline"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
K8S_DIR="$REPO_ROOT/k8s"
SIMULATOR_DIR="$REPO_ROOT/simulators"
FLINK_DIR="$REPO_ROOT/flink"

info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m    $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
err()   { echo -e "\033[1;31m[ERR]\033[0m   $*" >&2; exit 1; }

wait_for_deploy() {
  local name=$1
  info "Waiting for deployment/$name..."
  kubectl rollout status deployment/"$name" -n "$NAMESPACE" --timeout=180s \
    && ok "$name is ready" \
    || warn "$name timed out — check: kubectl get pods -n $NAMESPACE"
}

if [[ "${1:-}" == "teardown" ]]; then
  info "Tearing down namespace $NAMESPACE..."
  kubectl delete namespace "$NAMESPACE" --ignore-not-found
  ok "Teardown complete"
  exit 0
fi

command -v minikube >/dev/null || err "minikube not found"
command -v kubectl  >/dev/null || err "kubectl not found"
command -v docker   >/dev/null || err "docker not found"

if ! minikube status | grep -q "Running"; then
  warn "Minikube not running — starting..."
  minikube start --driver=docker --cpus=4 --memory=8192 --disk-size=30g
fi

# Build and load images
info "Building machine-simulator image..."
docker build -t machine-simulator:latest "$SIMULATOR_DIR"
minikube image load machine-simulator:latest
ok "machine-simulator loaded"

info "Building flink-pipeline image..."
docker build -t flink-pipeline:latest "$FLINK_DIR"
minikube image load flink-pipeline:latest
ok "flink-pipeline loaded"

# Apply manifests in dependency order
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

info "Submitting Flink pipeline job..."
kubectl delete job flink-job-submit -n "$NAMESPACE" --ignore-not-found
kubectl apply -f "$K8S_DIR/flink/flink-job.yaml"

info "Deploying Grafana..."
kubectl apply -f "$K8S_DIR/grafana/grafana.yaml"
wait_for_deploy grafana

info "Deploying Simulators..."
kubectl apply -f "$K8S_DIR/simulators/simulator.yaml"
wait_for_deploy simulator

MINIKUBE_IP=$(minikube ip)
echo ""
echo "======================================================"
ok "Stack deployed!"
echo "  Grafana:   http://$MINIKUBE_IP:30300  (admin/admin)"
echo "  Flink UI:  http://$MINIKUBE_IP:30081"
echo "  IoTDB:     $MINIKUBE_IP:30667"
echo ""
echo "  Verify pipeline:"
echo "    kubectl logs -n $NAMESPACE job/flink-job-submit"
echo "    curl -s http://$MINIKUBE_IP:30081/v1/jobs | python3 -m json.tool"
echo "======================================================"

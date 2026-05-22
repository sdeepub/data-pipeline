#!/usr/bin/env bash
# =============================================================
# deploy.sh  —  Deploy data-pipeline stack to Minikube
# Usage:
#   ./scripts/deploy.sh          # full deploy
#   ./scripts/deploy.sh teardown # delete everything
# =============================================================

set -euo pipefail

NAMESPACE="data-pipeline"
K8S_DIR="$(cd "$(dirname "$0")/../k8s" && pwd)"
SIMULATOR_DIR="$(cd "$(dirname "$0")/../simulators" && pwd)"

# ── helpers ───────────────────────────────────────────────────
info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m    $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
err()   { echo -e "\033[1;31m[ERR]\033[0m   $*" >&2; exit 1; }

wait_for_deploy() {
  local name=$1
  info "Waiting for deployment/$name to be ready..."
  kubectl rollout status deployment/"$name" -n "$NAMESPACE" --timeout=120s \
    && ok "$name is ready" \
    || warn "$name timed out — check: kubectl get pods -n $NAMESPACE"
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

if ! minikube status | grep -q "Running"; then
  warn "Minikube doesn't appear to be running. Starting it..."
  minikube start --memory=6144 --cpus=4 --driver=docker
fi

# ── build & load simulator image ─────────────────────────────
info "Building machine-simulator image..."
docker build -t machine-simulator:latest "$SIMULATOR_DIR"
info "Loading image into Minikube (avoids registry push)..."
minikube image load machine-simulator:latest
ok "Simulator image loaded"

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

info "Deploying Flink..."
kubectl apply -f "$K8S_DIR/flink/flink.yaml"
wait_for_deploy flink-jobmanager
wait_for_deploy flink-taskmanager

info "Deploying Grafana..."
kubectl apply -f "$K8S_DIR/grafana/grafana.yaml"
wait_for_deploy grafana

info "Deploying Simulator..."
kubectl apply -f "$K8S_DIR/simulator/simulator.yaml"

# ── print access URLs ─────────────────────────────────────────
MINIKUBE_IP=$(minikube ip)

echo ""
echo "======================================================"
ok "All manifests applied!"
echo ""
echo "  Service          URL"
echo "  ──────────────── ────────────────────────────────"
echo "  Grafana          http://$MINIKUBE_IP:30300  (admin/admin)"
echo "  Flink UI         http://$MINIKUBE_IP:30081"
echo "  IoTDB Thrift     $MINIKUBE_IP:30667"
echo "  Kafka (external) $MINIKUBE_IP:30092"
echo ""
echo "  Or use port-forward:"
echo "    kubectl port-forward svc/grafana 3000:3000 -n $NAMESPACE"
echo "    kubectl port-forward svc/flink-jobmanager 8081:8081 -n $NAMESPACE"
echo "    kubectl port-forward svc/kafka 9092:9092 -n $NAMESPACE"
echo ""
echo "  Scale simulators:"
echo "    kubectl scale deploy/simulator --replicas=10 -n $NAMESPACE"
echo ""
echo "  Logs:"
echo "    kubectl logs -l app=kafka -n $NAMESPACE --tail=50"
echo "    kubectl logs -l app=simulator -n $NAMESPACE --tail=20"
echo "======================================================"

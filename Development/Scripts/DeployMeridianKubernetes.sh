#!/usr/bin/env bash
set -euo pipefail

# Uses the local Docker Desktop engine and Kubernetes context.
# The properties file must include the settings from meridian-spark-defaults.conf
# plus the deployment's approved ADLS authentication configuration.
: "${MERIDIAN_SPARK_PROPERTIES:?Set MERIDIAN_SPARK_PROPERTIES to the private Spark properties file}"
namespace=${MERIDIAN_NAMESPACE:-meridian}
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"

[[ $(kubectl config current-context) == docker-desktop ]]
[[ -r $MERIDIAN_SPARK_PROPERTIES ]]

docker build --file Development/Docker/Dockerfile --target spark-jobs -t meridian-spark-jobs:0.1.0 .
docker build --file Development/Docker/Dockerfile --target kafka-producer -t meridian-kafka-producer:0.1.0 .

kube() { kubectl --context docker-desktop -n "$namespace" "$@"; }
kube create namespace "$namespace" --dry-run=client -o yaml | kube apply -f -
kube create secret generic meridian-spark-config \
  --from-file="spark-defaults.conf=$MERIDIAN_SPARK_PROPERTIES" --dry-run=client -o yaml |
  kube apply --server-side -f -
kube apply -f Development/Kubernetes/meridian-kafka-kubernetes.yaml
kube rollout status statefulset/meridian-kafka --timeout=5m
kube apply -f Development/Kubernetes/meridian-kubernetes.yaml
kube rollout status deployment/meridian-producer --timeout=5m
kube rollout status deployment/meridian-stream --timeout=10m
kube wait --for=condition=complete job/meridian-batch-check --timeout=10m
kube logs job/meridian-batch-check

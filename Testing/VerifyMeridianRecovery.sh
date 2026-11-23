#!/usr/bin/env bash
set -euo pipefail

namespace=${MERIDIAN_NAMESPACE:-meridian}
kube() { kubectl -n "$namespace" "$@"; }

pod=$(kube get pod -l app=meridian-stream -o jsonpath='{.items[0].metadata.name}')
[[ -n $pod ]]
before=$(kube logs "$pod")
# Query progress is emitted after the outputs and checkpoint commit complete.
last_batch=$(awk '/"batchId" : [0-9]+,/ { batch=$3; sub(/,/, "", batch); next }
  /"numInputRows" : [0-9]+,/ && batch != "" { if ($3+0 > 0) complete=batch; batch="" }
  END { if (complete != "") print complete }' <<< "$before")
[[ $last_batch =~ ^[0-9]+$ ]]
next_batch=$((last_batch + 1))

kube delete pod "$pod" --wait=true --timeout=2m
for ((attempt=0; attempt<120; attempt++)); do
  replacement=$(kube get pod -l app=meridian-stream -o jsonpath='{.items[0].metadata.name}')
  if [[ -n $replacement && $replacement != "$pod" ]]; then
    after=$(kube logs "$replacement" 2>/dev/null || true)
    if [[ $after == *"Resuming at batch $next_batch with committed offsets"* ]]; then
      printf 'Replacement %s resumed at batch %s after %s committed batch %s.\n' \
        "$replacement" "$next_batch" "$pod" "$last_batch"
      exit 0
    fi
  fi
  sleep 5
done
printf 'Replacement stream did not confirm checkpoint recovery at batch %s.\n' "$next_batch" >&2
exit 1

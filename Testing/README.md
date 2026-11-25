# Validation Tests and CI Evidence

| Check | Evidence |
| --- | --- |
| Historical data quality | [Shipment reconciliation](../Logging/meridian-batch-kubernetes.log) and [malformed ID and Delta upsert regression](evidence/meridian-delta-upsert-verification.log) |
| KPI formats | [Delta and Iceberg parity](evidence/databricks-kpi-comparison.csv) |
| Iceberg schema evolution | [14 before and 14 after](evidence/databricks-schema-evolution-row-counts.png) |
| ADLS storage and recovery | [Read/write probe](../Logging/meridian-adls-write-verification.log) and [pod recovery and snapshot comparison](../Logging/meridian-adls-recovery-verification.log) |
| Kubernetes operations | [Rolling update and rollback](evidence/kubernetes-producer-rollout.log) |
| Hosted CI | [Workflow source](../.github/workflows/ci.yml) and [latest runs on main](https://github.com/ShanmukhaAaryasree/project-meridian/actions/workflows/ci.yml?query=branch%3Amain) |
| Local live-stack configuration | `docker compose -f Development/Docker/compose.realtime.yaml config --quiet` (also checked by CI) |

The [local validation script](validate-project.ps1) checks the producer image and archived batch and streaming results. See the [requirements matrix](../docs/requirements-compliance.md#verification-scope) for the verification scope and limitations.

The [latest workflow runs](https://github.com/ShanmukhaAaryasree/project-meridian/actions/workflows/ci.yml?query=branch%3Amain) show separate producer-image and processing-image jobs. The processing job includes the packaged batch reconciliation.

CI validates the Compose file and builds the stream image; it does not start Kafka and Spark together or assert a live-event latency target. Use the [local real-time walkthrough](../README.md#run-real-time-streaming-locally) for an end-to-end event demo.

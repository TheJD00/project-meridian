# Historical Batch and Kafka Ingestion

| Path | Implementation |
| --- | --- |
| Historical CSVs | [Dataset pack](../Datasets/README.md) → [Databricks notebook](Batch/01_Shipments_Batch.scala) or [portable Spark batch](Batch/meridian-batch.scala) |
| Tracking fixture | [Keyed fixture](../Datasets/streaming/tracking_events_kafka_keyed.txt) → [Kafka producer image](../Development/Docker/meridian-producer.Dockerfile) → [broker](../Development/Kubernetes/meridian-kafka.yaml) |
| Live consumption | [Spark consumer](Streaming/meridian-stream.scala) → Iceberg replay, Delta audit, and Iceberg windows |
| Local continuous demo | [Docker Compose stack](../Development/Docker/compose.realtime.yaml) → [start script](../Development/Scripts/start-meridian-realtime.ps1) → [event publisher](../Development/Scripts/publish-meridian-event.ps1) |
| Tracking export audit | [43-event JSONL export](../Datasets/streaming/meridian-kafka-events.jsonl) → tracking audit cells in the [Databricks notebook](Batch/01_Shipments_Batch.scala) |

The finite replay uses `Trigger.AvailableNow`; continuous mode polls Kafka with a configurable two-second processing-time trigger by default. The live Compose demo uses its own topic and persistent volumes. A trigger interval controls scheduling, not guaranteed end-to-end latency. Use a fresh topic and matching checkpoint for independent runs. See the [local live setup](../README.md#run-real-time-streaming-locally) and [Kubernetes replay instructions](../README.md#run-the-kubernetes-replay).

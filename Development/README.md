# Docker and Kubernetes Setup

| Workload | Source | Image | Kubernetes |
| --- | --- | --- | --- |
| Kafka replay producer | [Keyed data](../Datasets/streaming/tracking_events_kafka_keyed.txt) | [Dockerfile](Docker/meridian-producer.Dockerfile) | [Producer Deployment](Kubernetes/meridian-producer-deployment.yaml) |
| Portable Spark batch | [Scala](../Data%20Ingestion/Batch/meridian-batch.scala) | [Dockerfile](Docker/meridian-batch.Dockerfile) | [Batch Job](Kubernetes/meridian-batch-job.yaml) |
| Spark streaming | [Scala](../Data%20Ingestion/Streaming/meridian-stream.scala), [launcher](../Data%20Ingestion/Streaming/meridian-stream.sh) | [Dockerfile](Docker/meridian-stream.Dockerfile) | [Stream Deployment](Kubernetes/meridian-stream-deployment.yaml) |
| Local continuous demo | [Compose stack](Docker/compose.realtime.yaml), [start script](Scripts/start-meridian-realtime.ps1), [event publisher](Scripts/publish-meridian-event.ps1) | Kafka and Spark containers | — |

The [Kafka manifest](Kubernetes/meridian-kafka.yaml) provisions the Kubernetes broker, Service and streaming PVC. Use the [Kubernetes launch script](Scripts/start-meridian-live.ps1) for a dedicated Kubernetes or ADLS run. The [local Compose launch script](Scripts/start-meridian-realtime.ps1) starts a local broker and continuous consumer; publish events with the paired publisher script. The processing-time trigger defaults to two seconds and can be configured with `MERIDIAN_TRIGGER_INTERVAL`; actual event-to-output latency depends on batch processing time. The [ADLS probe](Kubernetes/meridian-adls-probe.yaml) checks authenticated reads and writes. [Build and run steps](../README.md#run-real-time-streaming-locally)

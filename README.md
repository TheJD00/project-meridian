# Project Meridian

**Unified logistics intelligence and real-time shipment tracking**

## Project overview

Meridian cleans historical shipment data, publishes delivery KPIs, replays tracking fixtures, and supports continuous Kafka-to-Spark Structured Streaming for new shipment events. The capstone brief describes 8,768 shipment records. The trainer supplied a mini pack with 141 shipment rows, including eight re-sent versions. The reported batch results cover that mini pack. Full-dataset and production-volume validation require a separate run.

Start with the [architecture](#architecture), [demo guide](docs/demo-guide.md), or [requirements and evidence matrix](docs/requirements-compliance.md).

## Technology stack

| Area | Technology |
| --- | --- |
| Batch processing | Azure Databricks, Scala, Apache Spark, Unity Catalog |
| Streaming | Apache Kafka and Spark Structured Streaming on local Kubernetes |
| Storage and table formats | ADLS Gen2, local persistent volume, Delta Lake, Apache Iceberg |
| Deployment | Docker, Kubernetes Jobs and Deployments, ConfigMaps and Secrets |
| Analytics and verification | Databricks SQL, Spark SQL, GitHub Actions |

## Repository contents

| Section | What to open |
| --- | --- |
| [Architecture](Architecture/README.md) | Batch and streaming diagrams, deployment boundaries, and shipment model |
| [Datasets](Datasets/README.md) | Trainer-supplied CSVs, data dictionary, and tracking fixtures |
| [Data Ingestion](Data%20Ingestion/README.md) | Historical notebook, Kafka producer, and streaming consumer |
| [Dashboard](Dashboard/README.md) | Bronze, Silver, and Gold tables, business queries, and findings |
| [Development](Development/README.md) | Workload images, Kubernetes manifests, and run commands |
| [Logging and monitoring](Logging/README.md) | Batch, replay, ADLS recovery, and Kafka lag evidence |
| [Testing](Testing/README.md) | Data quality, format parity, schema evolution, rollout, and hosted CI |
| [Demo documentation](docs/demo-guide.md) | Walkthrough, run commands, and evidence |

## Architecture

```mermaid
flowchart LR
  CSV["Four historical CSVs"] --> DB["Azure Databricks · Scala Spark"]
  DB --> D["Delta · raw, curated and rejected data"]
  DB --> G["Four KPI tables · Delta and Iceberg"]
  R["Keyed tracking replay"] --> K["Kafka on Kubernetes"]
  K --> S["Spark Structured Streaming"]
  S --> I["Iceberg · replay records and 5-minute windows"]
  S --> A["Delta · live event dispositions"]
  S --> P["Persistent volume or ADLS Gen2 · warehouse and checkpoint"]
  K --> E["Kafka export"]
  E --> DB
```

See the [shipment model and deployment boundaries](Architecture/README.md) for the relationships behind the KPI tables.

## Dataset

| Input | Data rows |
| --- | ---: |
| Shipments | 141 |
| Customers | 78 |
| Carriers | 13 |
| Hubs | 25 |
| Tracking fixture | 43 |

The [data dictionary](Datasets/data_dictionary.csv) describes the source fields. The ADLS acceptance test added two synthetic events to the 43-event fixture.

## Medallion architecture and analytics

| Layer | Meridian output |
| --- | --- |
| Bronze | Raw Delta tables for shipments, customers, carriers, and hubs |
| Silver | Curated and rejected shipment Delta tables with traceable validation rules |
| Gold | Monthly delivery, carrier SLA, lane performance, and delay-reason KPIs in Delta and Iceberg |

The shipment-grain analytics view joins customer, carrier, origin hub, and destination hub references. See the [star-style model](Architecture/README.md#shipment-analytics-model) and [KPI catalogue](Dashboard/README.md#gold-kpi-tables).

## Run the project

Run the Docker and PowerShell commands from the repository root.

## Run real-time streaming locally

The Docker Compose demo starts a local Kafka broker and a continuous Spark Structured Streaming consumer. From the repository root, run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\Development\Scripts\start-meridian-realtime.ps1
docker compose -f Development/Docker/compose.realtime.yaml ps
docker compose -f Development/Docker/compose.realtime.yaml logs -f stream
```

Publish a new tracking event from another PowerShell window:

```powershell
$eventTime = [DateTime]::UtcNow
$event = @{
  event_id = 'EVT9999002'
  shipment_id = 'SHP2026008426'
  hub_id = 'HUB06'
  event_type = 'ROUTE_CHECKPOINT'
  event_time = $eventTime.ToString('yyyy-MM-ddTHH:mm:ssZ')
  ingest_time = $eventTime.AddSeconds(1).ToString('yyyy-MM-ddTHH:mm:ssZ')
  vehicle_id = 'VEH-1054'
  delay_minutes = 0
} | ConvertTo-Json -Compress
powershell -NoProfile -ExecutionPolicy Bypass -File .\Development\Scripts\publish-meridian-event.ps1 -EventJson $event
```

The continuous consumer uses a two-second processing-time trigger by default. Set `MERIDIAN_TRIGGER_INTERVAL` to change the trigger interval and `MERIDIAN_MAX_OFFSETS_PER_TRIGGER` to cap records read in a micro-batch. The trigger interval is not an end-to-end latency guarantee; actual delivery depends on how long each batch takes to process and the resources available. The Kafka topic, checkpoint and warehouse are separate from the finite replay. `docker compose -f Development/Docker/compose.realtime.yaml down` stops the containers and preserves their named volumes.

## Run the batch notebook

1. Open a Scala notebook on Azure Databricks classic compute. For the managed Iceberg steps, use Databricks Runtime 16.4 LTS or above and an output schema with predictive optimization enabled.
2. Upload the four CSVs under `Datasets/raw` to the same Unity Catalog volume folder. Import `Data Ingestion/Batch/01_Shipments_Batch.scala` as a source notebook. Set the **Input volume** and **Output catalog and schema** widgets for your workspace.
3. Run the batch, KPI and Iceberg cells in order. Use a dedicated writable schema for verification. The notebook merges existing curated shipments and refreshes the other Meridian outputs. Run the tracking audit cells after uploading the [Kafka export](Datasets/streaming/meridian-kafka-events.jsonl) alongside the CSVs.

The batch starts with 141 shipment rows and retains 133 latest shipment versions before the quality split. The four KPI outputs cover monthly delivery performance, carrier SLA, lane performance, and delay reasons.

Run `Dashboard/meridian-business-queries.sql` in Databricks SQL for shipment counts by status and monthly freight revenue by customer segment.

## Run the portable batch job

Build `docker build -f Development/Docker/meridian-batch.Dockerfile -t meridian-batch:v1 .` and run `docker run --rm meridian-batch:v1` for a local smoke test. To run it on Kubernetes, make the image available to the node and apply `Development/Kubernetes/meridian-batch-job.yaml`. Use `kubectl --context docker-desktop logs job/meridian-batch` for the completion counts. The packaged job writes four raw Delta datasets and the curated and rejected shipment datasets under `/tmp/meridian-batch` inside the container. Its default output is temporary, while the Databricks tables retain the published outputs.

## Run the Kubernetes replay

Prerequisites are Docker Desktop with Kubernetes enabled, `kubectl`, a Kafka Service named `meridian-kafka`, and a writable `ReadWriteOnce` PVC named `meridian-stream-state`. The demo used the `docker-desktop` context and the default namespace.

Apply `kubectl --context docker-desktop apply -f Development/Kubernetes/meridian-kafka.yaml` to create the demo broker, Service, and streaming PVC. The broker is disposable and the PVC stores the consumer state.

1. Use a single Kafka partition for `shipment-tracking-events`. Replay the supplied keyed text file with Kafka's console producer and `key.separator=|`. Confirm the topic end offset is 43 before starting Spark.
2. Build the versioned Spark image and make it available to the Kubernetes node. Create the consumer ConfigMap and apply the deployment from the repository root.

   ```powershell
   docker build -f Development/Docker/meridian-stream.Dockerfile -t meridian-stream:v1 .
   kubectl --context docker-desktop create configmap meridian-stream `
     --from-literal=KAFKA_BOOTSTRAP_SERVERS=meridian-kafka:9092 `
     --from-literal=KAFKA_TOPIC=shipment-tracking-events
   kubectl --context docker-desktop apply -f Development/Kubernetes/meridian-stream-deployment.yaml
   kubectl --context docker-desktop rollout status deployment/meridian-stream --timeout=300s
   kubectl --context docker-desktop logs deployment/meridian-stream
   ```

3. For the producer deployment, build `docker build -f Development/Docker/meridian-producer.Dockerfile -t meridian-producer:v1 .`. Make the image available to the Kubernetes node, create the single-partition topic `shipment-tracking-producer-demo`, and apply `Development/Kubernetes/meridian-producer-deployment.yaml`. The separate topic lets the producer scaling demo run independently of the consumer recovery test. Each new producer replica publishes a full 43-event replay.
4. For recovery, save the consumer logs, replace the Spark pod, and wait for the deployment to become ready. Compare its output with the first run. The same checkpoint should yield zero new input records, 43 persisted records, and identical windows.
5. For the Databricks audit, consume 43 values from `shipment-tracking-events` in order, save them as `meridian-kafka-events.jsonl`, and upload the file to the notebook's input volume.

The initial local replay and its replacement both retained 43 raw Kafka records. The replacement processed zero new records and reproduced the same 12 windows. The separate ADLS run started with the same 43-event fixture plus one deliberately late event. It recovered 44 raw records after pod replacement, then processed one additional accepted event. The final 45 raw records comprise 33 accepted, 7 duplicate, 4 rejected, and 1 late. See the [reproduction steps](docs/demo-guide.md#reproduce-the-adls-event-counts) and [ADLS verification log](Logging/meridian-adls-recovery-verification.log).

## Run the continuous consumer on Kubernetes

With the Kubernetes Kafka broker and PVC running, use `powershell -NoProfile -ExecutionPolicy Bypass -File .\Development/Scripts/start-meridian-live.ps1`. This creates a separate `shipment-tracking-live` topic and deploys a Spark consumer. It uses a two-second processing-time trigger by default; set `MERIDIAN_TRIGGER_INTERVAL` to change it. Batch execution time and available resources determine end-to-end latency. The consumer uses a separate checkpoint and Iceberg warehouse so the finite replay remains reproducible. Publish keyed events to the live topic to see new micro-batches in `kubectl --context docker-desktop logs -f deployment/meridian-stream-live`.

To use ADLS Gen2, provision an authorized Entra service principal and put its tenant ID, app client ID, and client secret in the `meridian-adls-auth` Kubernetes Secret as `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`. Use `Development/Kubernetes/meridian-adls-probe.yaml` to verify a Spark pod can write and read the project storage folder. Then pass `-Adls` to `Development/Scripts/start-meridian-live.ps1`. That mode uses its own Kafka topic and writes its checkpoint, Iceberg tables, and Delta event audit under the ADLS `meridian/streaming` prefix. Use `-RunId <suffix>` to start an independent run with a separate deployment, topic, checkpoint and warehouse. Keep the topic and storage paths consistent across pod replacements.

The streaming Deployment uses `Recreate` so Kubernetes terminates the old pod before starting its replacement during a Deployment update. One Spark writer owns the checkpoint and warehouse at a time, including when using ADLS. The update creates a short processing pause, then the replacement resumes from the durable checkpoint. The stateless producer uses a rolling update. Its update and rollback are in the [Kubernetes rollout log](Testing/evidence/kubernetes-producer-rollout.log).

## Assumptions and limits

- Batch keeps the newest shipment version by `last_updated` and the earliest signup date for repeated customer IDs. When those fields tie, lexical order of the serialized source row gives a deterministic result. Malformed shipment IDs go to the rejected table and are retained individually. Exact carrier and hub duplicates collapse. If distinct rows share an ID, the corresponding shipments are rejected because the reference data has no version timestamp. Historical shipments may still reference inactive hubs or carriers. Blank distance is permitted. Delivered and returned shipments require a completion date.
- Each batch input is a complete historical snapshot. After initial publication, the curated Delta table merges changed rows by shipment ID, inserts new rows, and removes rows absent from the current validated snapshot. Supply a complete snapshot when rerunning the batch.
- Delivery KPIs use delivered shipments and shipment month. Revenue and volume exclude cancellations. The streaming replay uses UTC event time, five-minute windows, and a five-minute arrival delay threshold. Clock-skewed events are flagged. The first valid copy of an event ID wins in the single-partition replay.
- Delta stores batch layers and the live event audit. The consumer writes one Delta table at `event_audit` under the streaming warehouse, partitioned by disposition. The Databricks notebook can also create separate audit tables from a Kafka export. Iceberg stores published KPIs, raw Kafka replay records and streaming aggregates.
- The Kubernetes consumer supports finite replay and continuous modes. The versioned, non-root image includes its Scala code and Kafka, Delta, Iceberg, and ADLS dependencies. A failed continuous query exits so Kubernetes can restart it from its configured checkpoint.
- GitHub Actions builds the three workload images and checks the producer fixture count, non-root users, streaming dependencies, and packaged batch reconciliation.
- For larger datasets, broadcast sufficiently small reference tables to reduce shipment join shuffles. Replace full window recomputation with incremental updates and compact small Iceberg files as the number of writes grows.

## Delta Lake and Iceberg

Delta Lake fits the Databricks quality layers because the notebook can use native Spark writes and transaction history to trace curation and update shipments by key. Iceberg fits the published KPI and tracking outputs because analytics engines that support the Iceberg catalog can read the same snapshots and evolved schema. Publishing in both formats gives a portable analytics copy and requires extra storage, writes, and a parity check. The notebook compares all four published Iceberg KPI tables with their Delta sources.

The [original capstone brief](docs/PROJECT_MERIDIAN_Capstone_Logistics_Intelligence_Real-Time_Tracking.docx) is in `docs/`.

## Validate locally

Run `powershell -NoProfile -ExecutionPolicy Bypass -File .\Testing/validate-project.ps1` with Docker Desktop running. It rebuilds and checks the producer image, then reads the archived batch and recovery logs. For a fresh execution, run the batch and streaming walkthroughs above. See the [verification scope](docs/requirements-compliance.md#verification-scope) for the distinction between archived results and the latest rerun attempt.

## Hosted CI

Both `producer-image` and `processing-images` passed on the merged `main` commit `8ea5f356227f3925e4c6b878212a143fb66a7c30`. The processing job built the batch and stream images, validated the real-time Compose configuration, and passed batch reconciliation. CI does not run the live Kafka-to-Spark event demonstration or verify an end-to-end latency target. [Successful Meridian CI run](https://github.com/ShanmukhaAaryasree/project-meridian/actions/runs/45182186442) · [Latest runs on main](https://github.com/ShanmukhaAaryasree/project-meridian/actions/workflows/ci.yml?query=branch%3Amain)

## Recorded outcomes

- Batch reconciled **141 raw → 133 latest → 122 curated + 11 rejected**. The Delta merge test updated one curated row. [Batch log](Logging/meridian-batch-kubernetes.log) · [Merge evidence](Testing/evidence/meridian-delta-upsert-verification.log)
- Four Gold KPI tables matched across Delta and Iceberg. Adding `delay_category` preserved **14 rows**. [KPI comparison](Testing/evidence/databricks-kpi-comparison.csv) · [Schema screenshot](Testing/evidence/databricks-schema-evolution-row-counts.png)
- The ADLS-backed replacement recovered **44 records and 12 windows**, then processed one more event. [Recovery log](Logging/meridian-adls-recovery-verification.log) · [Kafka lag](Testing/evidence/meridian-kafka-lag.csv)

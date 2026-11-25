# Batch and Streaming Architecture

## Batch flow

```mermaid
flowchart LR
  Files["Shipment, customer, carrier and hub CSVs"] --> Volume["Unity Catalog volume"]
  Volume --> Spark["Scala Spark on Azure Databricks"]
  Spark --> Bronze["Bronze · raw Delta"]
  Bronze --> Silver["Silver · curated Delta"]
  Bronze --> Reject["Rejected Delta"]
  Silver --> Gold["Gold · four KPI tables"]
  Gold --> Delta["Delta outputs"]
  Gold --> Iceberg["Iceberg KPI copies"]
```

## Streaming flow

```mermaid
flowchart LR
  Producer["Tracking event publisher"] --> Broker["Kafka on Docker Compose or Kubernetes"]
  Broker --> Spark["Spark Structured Streaming"]
  Spark --> Replay["Iceberg replay records"]
  Replay --> Audit["Delta disposition audit"]
  Audit --> Windows["Iceberg five-minute windows"]
  Spark --> Checkpoint["Spark checkpoint"]
```

The finite replay and continuous live consumer use separate topics, checkpoints, and warehouses. The local live demo runs Kafka and Spark with Docker Compose; Kubernetes supports the persistent-volume and ADLS-backed deployment paths. Continuous mode uses a two-second processing-time trigger by default, while actual end-to-end delay depends on micro-batch processing time. The ADLS acceptance run stores its checkpoint, Iceberg warehouse, and Delta audit in ADLS Gen2 while Kafka and Spark compute remain on local Kubernetes. See the [replay and recovery record](../Logging/meridian-adls-recovery-verification.log).

## Shipment analytics model

```mermaid
flowchart TB
  Shipment["Curated shipment · one row per accepted shipment"]
  Customer["Customer · segment"] --> Shipment
  Carrier["Carrier · SLA target"] --> Shipment
  Origin["Hub reference · origin role"] --> Shipment
  Destination["Hub reference · destination role"] --> Shipment
  Shipment --> View["meridian_shipment_analytics"]
```

The Databricks notebook builds a star-style analytics view from the curated shipment table and four reference roles. The two hub aliases use the same source reference. [Batch notebook](../Data%20Ingestion/Batch/01_Shipments_Batch.scala) · [KPI catalogue](../Dashboard/README.md)

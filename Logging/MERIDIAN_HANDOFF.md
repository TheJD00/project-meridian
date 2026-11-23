# Meridian pipeline handoff

Updated 2026-11-23. Azure Databricks batch verification passed. The streaming job and deployment automation are in the workspace. Local Spark fixture replay and checkpoint recovery passed. Live Kafka, Kubernetes, ADLS streaming, and hosted CI execution are pending.

## Completed Azure Databricks batch work

- Workspace and Unity Catalog catalog `adlsgen2ws`, schema `meridian`, and external location `adlsgen2extloc`. Storage account `adlsgen2jd`, container `adlsgen2jdcontainer`, project prefix `meridian/`. The external location uses `azureadlsgen2cred`.
- The user ran the imported Scala notebook interactively on Personal Compute in the Databricks browser UI. The displayed successful runs were 11:06 AM and 11:16 AM on November 23. The second run had `stage_sample=false` and reused the staged files. Cell 4 completed all five Delta–Iceberg KPI comparisons on both runs. [Batch notebook and audit cell](https://adb-7405619332083831.11.azuredatabricks.net/editor/notebooks/2572625941060681?o=7405619332083831#command/8367413974263813)
- Cell 5 verified shipment Silver Delta history. Version 0 was `WRITE`, and versions 1 and 2 were `MERGE`. [Delta history evidence](https://adb-7405619332083831.11.azuredatabricks.net/editor/notebooks/2572625941060681?o=7405619332083831#command/8367413974263816)
- Cell 6 confirmed Iceberg format for `adlsgen2ws.meridian.monthly_delivery`. Cell 7 ran the history query. The screenshot of cell 6 truncated the storage path. Expand the location value and save the full path and the history rows for the submission. [Iceberg detail and history](https://adb-7405619332083831.11.azuredatabricks.net/editor/notebooks/2572625941060681?o=7405619332083831#command/8367413974263818)
- Cells 8–10 created `adlsgen2ws.meridian.iceberg_schema_demo`, added `review_note STRING`, and displayed the new column with nulls on existing monthly rows. Cell 11 queried `monthly_delivery VERSION AS OF 2` and returned 11 monthly rows totaling 118 bookings. [Iceberg time-travel evidence](https://adb-7405619332083831.11.azuredatabricks.net/editor/notebooks/2572625941060681?o=7405619332083831#command/8367413974263822)
- Local Spark 3.5.9 and Scala 2.12 check mode matched the table below and evaluated all five KPI queries. [Batch source](../Data%20Ingestion/Batch/src/main/scala/meridian/BatchJob.scala), [generated notebook](../Data%20Ingestion/Batch/Databricks/MeridianBatch.scala), and [optional Databricks CLI submission script](../Data%20Ingestion/Batch/Databricks/DeployBatch.ps1).
- Cluster ID `1117-113723-6nsppt3v`. It was running when the screenshots were taken. Its state after the evidence collection is unverified. If it was started for this work and is still idle, terminate it in Databricks Compute.

| Entity | Input | Curated | Rejected or superseded |
| --- | ---: | ---: | ---: |
| Hubs | 25 | 23 | 2 |
| Carriers | 13 | 12 | 1 |
| Customers | 78 | 75 | 3 |
| Shipments | 141 | 118 | 23 |

## Completed streaming implementation and local verification

- [MeridianStreaming.scala](../Data%20Ingestion/Streaming/MeridianStreaming.scala) reads Kafka JSON or local fixtures, validates tracking events, classifies rejected, duplicate, and late records, and computes 15-minute per-hub aggregates. The late cutoff is the prior accepted event-time maximum minus 15 minutes. Kafka mode uses a Delta event ledger and Iceberg window upserts keyed by window start and hub. Use a single writer with the same checkpoint and ledger together.
- The streaming job logs Spark progress JSON after completed micro-batches and reports the next batch from the checkpoint commit log on startup. [VerifyMeridianRecovery.sh](../Testing/VerifyMeridianRecovery.sh) now detects this progress and resume evidence.
- [BuildMeridianStreaming.ps1](../Development/Scripts/BuildMeridianStreaming.ps1) packaged the stream, and the combined batch and stream sources compiled together. Local Spark read four fixture files in four committed micro-batches and classified all 43 records. The first two files were processed before restarting with the same checkpoint and adding the remaining two. A final restart with no new input resumed at batch 4 without creating another checkpoint commit. This used Spark file input and Parquet output.
- Final local counts were 13 accepted, 6 duplicate, 22 late, and 2 rejected. The shipping data is in [Datasets/streaming](../Datasets/streaming).
- [Dockerfile](../Development/Docker/Dockerfile) builds versioned Spark job and Kafka producer images. [compose.yaml](../Development/Docker/compose.yaml) supplies a local Kafka broker. [ReplayMeridianKafka.ps1](../Development/Scripts/ReplayMeridianKafka.ps1) is an independent replay and offset check for an existing Kafka container. [ReplayMeridianKafka.sh](../Development/Scripts/ReplayMeridianKafka.sh) is the container producer.
- [VerifyMeridianContainers.sh](../Testing/VerifyMeridianContainers.sh) builds the images and checks the batch job, Kafka offsets, Delta ledger, Iceberg window totals and snapshots, time travel, and checkpoint reuse. The keyed replay keeps all 43 events but places its far-future event at the end of the second 11-event batch, allowing two Iceberg snapshots before later events exercise the late cutoff. A live Docker run passed: the producer reached offset 43, ledger dispositions totaled 43 (16 accepted, 6 duplicate, 19 late, 2 rejected), window totals reconciled, the earlier snapshot query returned rows, and a no-input restart resumed at batch 4 without adding a checkpoint commit. [.github/workflows/meridian.yml](../.github/workflows/meridian.yml) runs this integration in GitHub Actions.
- [meridian-kafka-kubernetes.yaml](../Development/Kubernetes/meridian-kafka-kubernetes.yaml) defines the broker Service and StatefulSet. [meridian-kubernetes.yaml](../Development/Kubernetes/meridian-kubernetes.yaml) defines the producer and consumer Deployments, externalized settings, Spark UI Service, and containerized batch check Job. [DeployMeridianKubernetes.sh](../Development/Scripts/DeployMeridianKubernetes.sh) builds and deploys to Docker Desktop. [VerifyMeridianRecovery.sh](../Testing/VerifyMeridianRecovery.sh) replaces the streaming pod and checks that the replacement resumes at the next batch.

## Completed live Kubernetes deployment and recovery

- Docker Desktop Kubernetes deployment completed using the private Spark properties file. The supplied OAuth settings were combined locally with the repository's Spark package and catalog defaults; the merged file and credentials remain outside Git.
- The Docker Desktop node has about 3.7 GiB allocatable memory. The Spark driver and pod requests/limits in `meridian-kubernetes.yaml` were reduced to allow the stream and batch-check pods to schedule together.
- The `meridian-batch-check` Job completed successfully. Kafka reached offset 43 for `meridian-tracking-events`.
- The stream committed batches 0 through 3, read/wrote its ADLS Delta ledger and checkpoint, and became idle. `VerifyMeridianRecovery.sh` deleted the original pod; the replacement resumed at batch 4 after committed batch 3. The replacement is Ready with no restarts.
- A separate query of `meridian.stream.hub_windows` and its Iceberg snapshot history/time travel has not yet been captured.

## Remaining work in order

1. **Verify the live streaming Iceberg table.** Query `meridian.stream.hub_windows`, confirm that its window totals reconcile with accepted ledger events, and save its snapshot history plus a query against an earlier snapshot. The table's warehouse is under `meridian/stream/iceberg`.
2. **Assemble the capstone evidence.** Include the successful batch-check Job, Kafka end offset 43, completed stream batches, pod replacement/resume at batch 4, the Iceberg query results, and the passing CI run. Keep credentials and the private Spark properties file out of Git and screenshots.

## Session access and verification limits

The updated stream source compiled and the live Docker integration passed as detailed above. A direct Spark replay attempt on Windows failed in Hadoop's Windows native file-permission call; use the passing Linux container run as local streaming evidence instead. Live Kubernetes-to-ADLS processing and pod-replacement recovery have now passed. The streaming Iceberg table/snapshot query and final capstone evidence remain.

The public `TheJD00/project-meridian` repository is the destination. The live Kubernetes resource adjustment and deployment results are recorded here. Do not commit generated JARs or private ADLS authentication settings. The **Meridian integration** GitHub Actions run passed.

# Meridian batch pipeline

The Scala Spark job processes the four historical CSV files, validates reference data and shipments, and publishes five KPI datasets. The project uses sbt and can be imported into IntelliJ from this directory.

## Verify on local Spark

From the repository root with Java 17, sbt, and Spark 3.5.x with Scala 2.12:

```powershell
Push-Location 'Data Ingestion/Batch'
try {
  sbt package
  spark-submit --master "local[2]" --class meridian.BatchJob target/scala-2.12/meridian-batch_2.12-0.1.0.jar '..\..\Datasets\batch\raw' --check
} finally {
  Pop-Location
}
```

Check mode reconciles input, curated, and rejected row counts for each entity, checks curated key uniqueness, and prints sample KPI output.

## Run in Azure Databricks

1. Build the JAR from `Data Ingestion/Batch` with `sbt package` and attach it as a library to Scala-compatible Spark 3.5 compute using Scala 2.12.
2. Upload the four source folders from `Datasets/batch/raw` into the `meridian/source` prefix under external location `adlsgen2extloc`. Preserve the folder structure, such as `shipments/shipments.csv`.
3. Import `Data Ingestion/Batch/Databricks/RunBatch.scala`. Set `raw_root` to `abfss://adlsgen2jdcontainer@adlsgen2jd.dfs.core.windows.net/meridian/source` or the corresponding Unity Catalog external volume path. The notebook must run with permission to read the source and write the Delta destination under `meridian/batch`.
4. To publish the KPI tables to Iceberg as well, set `iceberg_namespace` to a writable Iceberg catalog and namespace, such as `iceberg.meridian`. The Iceberg catalog and namespace must already be configured on the compute and support Spark's DataFrameWriterV2 create-or-replace operation. An empty setting runs Delta publishing only.
5. Compare the displayed counts with `audit/batch_counts` and inspect the Delta tables under `bronze/`, `silver/`, `rejected/`, and `gold/`.

## Batch rules

- Process a complete source snapshot per run. Each Delta output is overwritten independently. An interrupted run can be rerun from the same input snapshot to refresh the outputs.
- Resolve reference duplicates among valid rows, preferring the latest signup date for customers and using the source record as a deterministic tie-breaker. Keep inactive hubs and carriers for historical joins.
- Rank shipment versions by the latest parseable `last_updated`. For timestamp ties, prefer the row with the fewest validation failures, then sort by source record. If the winning version fails validation, reject it and retain older versions as superseded.
- Preserve source fields and source file paths for lineage. Keep rejected rows with validation reasons. Record status and date repairs on accepted shipments. Keep missing distance as null and reject supplied invalid distances.
- Use delivered shipments for SLA and transit time. Exclude cancelled shipments from revenue and lane volume. The booking total counts all valid current shipment records. Attribute monthly KPIs to the booking month and retain returned shipments in eligible revenue.

## Verified mini-pack counts

Local Spark 3.5.9 completed check mode and evaluated all five KPI queries.

| Entity | Input | Curated | Rejected or superseded |
| --- | ---: | ---: | ---: |
| Carriers | 13 | 12 | 1 |
| Customers | 78 | 75 | 3 |
| Hubs | 25 | 23 | 2 |
| Shipments | 141 | 118 | 23 |

## Databricks Runtime 19 deployment

Use the generated `Data Ingestion/Batch/Databricks/MeridianBatch.scala` notebook on the existing Scala 2.13 compute. The JAR and the older `RunBatch.scala` runner target Spark 3.5 / Scala 2.12 compute. The generated notebook stages the mini pack by default, writes Delta and native Unity Catalog Iceberg outputs, and verifies that the KPI results agree across formats. Set `stage_sample=false` to read files already staged in the source volume. Rebuild the notebook with `Data Ingestion/Batch/Databricks/BuildNotebook.ps1` after changing the source.

`Data Ingestion/Batch/Databricks/DeployBatch.ps1` submits the generated notebook using the authenticated `meridian` profile. See `Logging/BATCH_HANDOFF.md` for the saved workspace state, permissions to verify, and outstanding live checks.

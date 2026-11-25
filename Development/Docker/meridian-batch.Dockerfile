FROM apache/spark:3.5.3-java17-scala AS batch-input
COPY ["Data Ingestion/Batch/meridian-batch.scala", "/app/meridian-batch.scala"]
COPY Datasets/raw /app/data

FROM apache/spark:3.5.3-java17-scala
COPY --from=batch-input /app /app
USER 185:185
ENTRYPOINT ["/bin/bash", "-ec", "exec /opt/spark/bin/spark-shell --master 'local[2]' --driver-memory 1g --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=4 --conf spark.databricks.delta.snapshotPartitions=4 --conf spark.jars.ivy=/tmp/meridian-ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --packages io.delta:delta-spark_2.12:3.2.1 -i /app/meridian-batch.scala </dev/null"]

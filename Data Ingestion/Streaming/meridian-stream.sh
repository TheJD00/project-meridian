#!/usr/bin/env bash
set -euo pipefail

ready=/tmp/meridian-stream-ready
rm -f "$ready"
jars=(/opt/meridian/jars/*.jar)
jar_list=$(IFS=,; echo "${jars[*]}")
spark_args=(
  --master 'local[2]'
  --driver-memory 1g
  --conf spark.ui.enabled=false
  --conf spark.sql.shuffle.partitions=4
  --conf spark.databricks.delta.snapshotPartitions=4
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
  --conf spark.sql.catalog.meridian=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.meridian.type=hadoop
  --conf "spark.sql.catalog.meridian.warehouse=${MERIDIAN_WAREHOUSE:-file:///meridian/warehouse}"
  --jars "$jar_list"
  -i /opt/meridian/meridian-stream.scala
)

if [[ "${MERIDIAN_STREAM_MODE:-}" == continuous ]]; then
  exec /opt/spark/bin/spark-shell "${spark_args[@]}" </dev/null
fi

/opt/spark/bin/spark-shell "${spark_args[@]}" </dev/null
test -f /tmp/meridian-stream-complete
touch "$ready"
exec sleep infinity

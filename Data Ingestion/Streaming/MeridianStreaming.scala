import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path

object MeridianStreaming {
  private val payload = new StructType()
    .add("event_id", StringType).add("shipment_id", StringType)
    .add("hub_id", StringType).add("event_type", StringType)
    .add("event_time", StringType).add("ingest_time", StringType)
    .add("vehicle_id", StringType).add("delay_minutes", IntegerType)
    .add("_corrupt_record", StringType)

  private val eventTypes = Seq("PICKED_UP", "DEPARTED_HUB", "ARRIVED_AT_HUB",
    "ROUTE_CHECKPOINT", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED", "EXCEPTION")

  private def classify(input: DataFrame, previous: DataFrame, batchId: Long): DataFrame = {
    val parsed = input.withColumn("event", from_json(col("raw_json"), payload))
      .select(col("*"), col("event.*")).drop("event")
      .withColumn("event_ts", expr("try_cast(event_time as timestamp)"))
      .withColumn("ingest_ts", expr("try_cast(ingest_time as timestamp)"))
      .withColumn("validation_error",
        when(col("_corrupt_record").isNotNull, "malformed_json")
          .when(!coalesce(col("event_id").rlike("^EVT[0-9]{7}$"), lit(false)), "invalid_event_id")
          .when(col("shipment_id").isNotNull && !col("shipment_id").rlike("^SHP2026[0-9]{6}$"), "invalid_shipment_id")
          .when(!coalesce(col("hub_id").rlike("^HUB[0-9]{2}$"), lit(false)), "invalid_hub_id")
          .when(!coalesce(col("event_type").isin(eventTypes: _*), lit(false)), "invalid_event_type")
          .when(col("event_ts").isNull || col("ingest_ts").isNull, "invalid_timestamp")
          .when(col("delay_minutes") < 0, "invalid_delay"))

    val accepted = previous.where(col("disposition") === "accepted")
    val priorIds = accepted.select(col("event_id"), lit(true).as("already_seen"))
    val cutoff = accepted.agg((max(col("event_ts")) - expr("INTERVAL 15 MINUTES")).as("late_cutoff"))
    val order = Window.partitionBy("event_id", "validation_error")
      .orderBy(col("event_ts"), col("ingest_ts"), col("source_partition"), col("source_offset"), col("raw_json"))

    parsed.join(priorIds, Seq("event_id"), "left").crossJoin(cutoff)
      .withColumn("copy_number", row_number().over(order))
      .withColumn("disposition",
        when(col("validation_error").isNotNull, "rejected")
          .when(col("already_seen") || col("copy_number") > 1, "duplicate")
          .when(col("event_ts") < col("late_cutoff"), "late")
          .otherwise("accepted"))
      .drop("_corrupt_record", "already_seen", "copy_number")
      .withColumn("batch_id", lit(batchId))
  }

  private def windows(ledger: DataFrame): DataFrame =
    ledger.where(col("disposition") === "accepted")
      .groupBy(window(col("event_ts"), "15 minutes"), col("hub_id"))
      .agg(count(lit(1)).as("event_count"),
        count(when(col("event_type").isin("DELAYED", "EXCEPTION"), 1)).as("delayed_or_exception_count"),
        avg(col("delay_minutes")).as("mean_delay_minutes"))
      .selectExpr("window.start AS window_start", "window.end AS window_end", "hub_id",
        "event_count", "delayed_or_exception_count", "mean_delay_minutes")

  private def nextBatchId(checkpoint: String, spark: SparkSession): Long = {
    val commits = new Path(checkpoint, "commits")
    val fs = commits.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(commits)) 0L
    else {
      val latest = fs.listStatus(commits).iterator
        .map(_.getPath.getName)
        .filter(_.matches("[0-9]+"))
        .map(_.toLong)
        .foldLeft(-1L)((current, batchId) => math.max(current, batchId))
      latest + 1L
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 4 || args.length == 6,
      "Usage: MeridianStreaming files <jsonl-dir> <checkpoint> <output> | kafka <brokers> <checkpoint> <delta-path> <topic> <iceberg-table>")
    val Array(mode, source, checkpoint, output) = args.take(4)
    require((mode == "files" && args.length == 4) || (mode == "kafka" && args.length == 6))
    val local = mode == "files"
    val icebergTable = if (local) None else Some(args(5))
    icebergTable.foreach { table =>
      require(table.matches("[a-zA-Z_][a-zA-Z_0-9]*(\\.[a-zA-Z_][a-zA-Z_0-9]*){1,2}"),
        "Use a qualified Iceberg table name with simple identifiers")
    }
    val spark = SparkSession.builder().appName("Meridian tracking stream").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val resumeBatchId = nextBatchId(checkpoint, spark)
    if (resumeBatchId > 0L) println(s"Resuming at batch $resumeBatchId with committed offsets")
    else println("Starting at batch 0 with no committed offsets")
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
        println(event.progress.prettyJson)
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    })
    val ledgerPath = if (local) s"$output/ledger" else output
    val format = if (local) "parquet" else "delta"
    val records = if (local) {
      spark.readStream.option("maxFilesPerTrigger", 1).text(source)
        .selectExpr("value AS raw_json", "CAST(NULL AS STRING) AS kafka_key",
          "CAST(NULL AS INT) AS source_partition", "CAST(NULL AS BIGINT) AS source_offset")
    } else {
      val maxOffsets = sys.env.getOrElse("MERIDIAN_MAX_OFFSETS", "10000")
      require(maxOffsets.toLong > 0, "MERIDIAN_MAX_OFFSETS must be positive")
      spark.readStream.format("kafka").option("kafka.bootstrap.servers", source)
        .option("subscribe", args(4)).option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", maxOffsets).load()
        .selectExpr("CAST(value AS STRING) AS raw_json", "CAST(key AS STRING) AS kafka_key",
          "partition AS source_partition", "offset AS source_offset")
    }
    icebergTable.foreach { table =>
      val namespace = table.substring(0, table.lastIndexOf('.'))
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace").collect()
    }
    val empty = records.limit(0).schema
    val emptyEvents = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], empty)
    val emptyHistory = emptyEvents.withColumn("event_id", lit(null).cast(StringType))
      .withColumn("event_ts", lit(null).cast(TimestampType))
      .withColumn("disposition", lit(null).cast(StringType))
    val initial = classify(emptyEvents, emptyHistory, 0L)
    initial.write.format(format).mode("ignore").save(ledgerPath)

    val writer = records.writeStream.option("checkpointLocation", checkpoint)
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val history = spark.read.format(format).load(ledgerPath)
        if (history.where(col("batch_id") === batchId).isEmpty && !batch.isEmpty) {
          val classified = classify(batch, history.where(col("batch_id") < batchId), batchId)
          val append = classified.write.format(format).mode("append")
          if (local) append.save(ledgerPath)
          else append.option("txnAppId", checkpoint).option("txnVersion", batchId).save(ledgerPath)
        }
        val committed = spark.read.format(format).load(ledgerPath)
        val summary = windows(committed)
        if (local) summary.write.mode("overwrite").parquet(s"$output/windows")
        else {
          val table = icebergTable.get
          val changed = windows(committed.where(col("batch_id") === batchId))
            .select("window_start", "hub_id")
          val updates = summary.join(changed, Seq("window_start", "hub_id"))
            .select(summary.columns.map(col): _*)
          if (!spark.catalog.tableExists(table)) summary.writeTo(table).using("iceberg").create()
          else if (!updates.isEmpty) {
            updates.createOrReplaceTempView("meridian_stream_window_updates")
            try spark.sql(s"""MERGE INTO $table AS target
              |USING meridian_stream_window_updates AS source
              |ON target.window_start = source.window_start AND target.hub_id = source.hub_id
              |WHEN MATCHED THEN UPDATE SET *
              |WHEN NOT MATCHED THEN INSERT *""".stripMargin).collect()
            finally spark.catalog.dropTempView("meridian_stream_window_updates")
          }
        }
        committed.groupBy("disposition").count().orderBy("disposition").show(false)
      }

    try {
      val availableNow = local || sys.env.get("MERIDIAN_AVAILABLE_NOW").contains("true")
      val query = (if (availableNow) writer.trigger(Trigger.AvailableNow()) else writer).start()
      query.awaitTermination()
    } finally spark.stop()
  }
}

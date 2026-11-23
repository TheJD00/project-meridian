// Databricks notebook source
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BatchJob {
  private val keys = Map(
    "hubs" -> "hub_id", "carriers" -> "carrier_id",
    "customers" -> "customer_id", "shipments" -> "shipment_id"
  )

  private def allowed(name: String, values: String*): Column =
    col(name).isin(values: _*)

  private def parseDate(name: String): Column =
    coalesce(
      expr(s"try_cast($name as date)"),
      expr(s"try_to_timestamp($name, 'dd-MM-yyyy')").cast("date")
    )

  private def read(spark: SparkSession, root: String, entity: String): DataFrame = {
    val source = spark.read.option("header", "true").csv(s"$root/$entity/$entity.csv")
    source.select(
      col("*"),
      col("_metadata.file_path").as("source_file"),
      to_json(struct(source.columns.map(col): _*), Map("ignoreNullFields" -> "false")).as("source_record")
    )
  }

  private def classify(frame: DataFrame, rules: (String, Column)*): DataFrame =
    frame.withColumn("reasons", filter(array(rules.map { case (reason, valid) =>
      when(!coalesce(valid, lit(false)), lit(reason))
    }: _*), reason => reason.isNotNull))

  private def deduplicate(frame: DataFrame, key: String, order: Seq[Column]): (DataFrame, DataFrame) = {
    val ranked = frame.withColumn("version_rank",
      row_number().over(Window.partitionBy(key).orderBy((order :+ col("source_record").asc): _*)))
    (ranked.filter(col("version_rank") === 1).drop("version_rank"),
      ranked.filter(col("version_rank") > 1).drop("version_rank")
        .withColumn("reasons", concat(col("reasons"), array(lit("duplicate_or_superseded")))))
  }

  private def normalize(raw: DataFrame): DataFrame = raw.select(raw.columns.map { name =>
    if (name.startsWith("source_")) col(name)
    else if (name.endsWith("_id")) upper(trim(col(name))).as(name)
    else trim(col(name)).as(name)
  }: _*)

  private def cast(frame: DataFrame, columns: (String, String)*): DataFrame =
    columns.foldLeft(frame) { case (data, (name, kind)) =>
      data.withColumn(name, expr(s"try_cast($name as $kind)"))
    }

  private def present(name: String): Column = length(col(name)) > 0

  private def dimensions(entity: String, raw: DataFrame): (DataFrame, DataFrame) = {
    val data = normalize(raw)
    val checked = entity match {
      case "hubs" =>
        classify(cast(data, "latitude" -> "decimal(12,6)", "longitude" -> "decimal(12,6)",
          "capacity_shipments_per_day" -> "int"),
          "invalid_hub_id" -> col("hub_id").rlike("^HUB[0-9]{2}$"),
          "missing_hub_details" -> (present("hub_name") && present("city") && present("state")),
          "invalid_region" -> allowed("region", "North", "South", "East", "West", "Central", "Northeast"),
          "invalid_hub_type" -> allowed("hub_type", "Gateway", "Regional", "Local"),
          "invalid_coordinates" -> (col("latitude").between(-90, 90) && col("longitude").between(-180, 180)),
          "invalid_capacity" -> (col("capacity_shipments_per_day") > 0),
          "invalid_active_flag" -> allowed("active", "Y", "N"))
      case "carriers" =>
        classify(cast(data, "rate_per_kg_km_inr" -> "decimal(18,6)", "sla_on_time_pct" -> "int"),
          "invalid_carrier_id" -> col("carrier_id").rlike("^CAR[0-9]{3}$"),
          "missing_carrier_name" -> present("carrier_name"),
          "invalid_transport_mode" -> allowed("transport_mode", "Road", "Rail", "Air"),
          "invalid_service_level" -> allowed("service_level", "Express", "Standard", "Economy"),
          "invalid_rate" -> (col("rate_per_kg_km_inr") > 0),
          "invalid_sla" -> col("sla_on_time_pct").between(0, 100),
          "invalid_active_flag" -> allowed("active", "Y", "N"))
      case "customers" =>
        classify(data.withColumn("signup_date", parseDate("signup_date"))
          .withColumn("city", initcap(col("city"))).withColumn("email", lower(col("email"))),
          "invalid_customer_id" -> col("customer_id").rlike("^CUST[0-9]{4}$"),
          "missing_customer_details" -> (present("customer_name") && present("city")),
          "invalid_email" -> col("email").rlike("^[^@\\s]+@[^@\\s]+[.][^@\\s]+$"),
          "invalid_segment" -> allowed("segment", "Enterprise", "SME", "E-commerce", "Retail"),
          "invalid_signup_date" -> col("signup_date").isNotNull)
    }
    val valid = checked.filter(size(col("reasons")) === 0)
    val order = if (entity == "customers") Seq(col("signup_date").desc) else Seq.empty
    val (accepted, duplicates) = deduplicate(valid, keys(entity), order)
    (accepted.drop("reasons"), checked.filter(size(col("reasons")) > 0).unionByName(duplicates))
  }

  private def shipments(raw: DataFrame, hubs: DataFrame, carriers: DataFrame,
                        customers: DataFrame): (DataFrame, DataFrame) = {
    val normalized = normalize(raw)
      .withColumn("distance_supplied", coalesce(present("distance_km"), lit(false)))
      .withColumn("delivery_supplied", coalesce(present("delivery_date"), lit(false)))
      .withColumn("raw_status", col("shipment_status"))
      .withColumn("shipment_status", regexp_replace(upper(col("shipment_status")), "[ -]+", "_"))
      .withColumn("shipment_status", when(col("shipment_status") === "DLVRD", "DELIVERED")
        .otherwise(col("shipment_status")))
      .withColumn("date_reformatted", coalesce(hasDayFirstDate, lit(false)))
    val typed = cast(normalized, "weight_kg" -> "decimal(18,3)", "distance_km" -> "decimal(18,3)",
      "freight_charge_inr" -> "decimal(18,2)", "last_updated" -> "timestamp")
      .withColumn("ship_date", parseDate("ship_date"))
      .withColumn("promised_date", parseDate("promised_date"))
      .withColumn("delivery_date", parseDate("delivery_date"))

    val joined = typed
      .join(broadcast(customers.select(col("customer_id"), col("segment"), col("customer_name"))),
        Seq("customer_id"), "left")
      .join(broadcast(carriers.select("carrier_id", "carrier_name", "sla_on_time_pct")),
        Seq("carrier_id"), "left")
      .join(broadcast(hubs.select(col("hub_id").as("origin_hub_id"), col("hub_name").as("origin_hub"))),
        Seq("origin_hub_id"), "left")
      .join(broadcast(hubs.select(col("hub_id").as("destination_hub_id"),
        col("hub_name").as("destination_hub"))), Seq("destination_hub_id"), "left")

    val checked = classify(joined,
      "invalid_shipment_id" -> col("shipment_id").rlike("^SHP2026[0-9]{6}$"),
      "unknown_customer" -> col("customer_name").isNotNull,
      "unknown_carrier" -> col("carrier_name").isNotNull,
      "unknown_origin_hub" -> col("origin_hub").isNotNull,
      "unknown_destination_hub" -> col("destination_hub").isNotNull,
      "same_origin_destination" -> (col("origin_hub_id") =!= col("destination_hub_id")),
      "invalid_ship_date" -> col("ship_date").isNotNull,
      "invalid_promised_date" -> (col("promised_date") >= col("ship_date")),
      "invalid_delivery_date" -> ((!col("delivery_supplied") && col("shipment_status") =!= "DELIVERED") ||
        (col("delivery_date") >= col("ship_date"))),
      "invalid_weight" -> (col("weight_kg") > 0),
      "invalid_distance" -> (!col("distance_supplied") || col("distance_km") > 0),
      "invalid_charge" -> (col("freight_charge_inr") >= 0),
      "invalid_status" -> allowed("shipment_status", "DELIVERED", "IN_TRANSIT", "BOOKED", "CANCELLED", "RETURNED"),
      "invalid_last_updated" -> col("last_updated").isNotNull)

    val invalidIdentity = checked.filter(!coalesce(col("shipment_id").rlike("^SHP2026[0-9]{6}$"), lit(false)) ||
      col("last_updated").isNull)
    val candidates = checked.filter(col("shipment_id").rlike("^SHP2026[0-9]{6}$") &&
      col("last_updated").isNotNull)
    val (latest, older) = deduplicate(candidates, "shipment_id",
      Seq(col("last_updated").desc, size(col("reasons")).asc))
    val accepted = latest.filter(size(col("reasons")) === 0)
      .withColumn("repairs", filter(array(
        when(col("date_reformatted"), lit("date_format")),
        when(col("raw_status") =!= col("shipment_status"), lit("shipment_status"))
      ), repair => repair.isNotNull))
      .drop("reasons", "raw_status", "date_reformatted", "distance_supplied", "delivery_supplied")
    (accepted, invalidIdentity.unionByName(latest.filter(size(col("reasons")) > 0)).unionByName(older))
  }

  private def hasDayFirstDate: Column =
    Seq("ship_date", "promised_date", "delivery_date")
      .map(name => coalesce(col(name).rlike("^[0-9]{2}-[0-9]{2}-[0-9]{4}$"), lit(false)))
      .reduce(_ || _)

  private def gold(shipments: DataFrame): Map[String, DataFrame] = {
    val delivered = col("shipment_status") === "DELIVERED"
    val eligible = col("shipment_status") =!= "CANCELLED"
    val facts = shipments
      .withColumn("ship_month", trunc(col("ship_date"), "month"))
      .withColumn("on_time", when(delivered, (col("delivery_date") <= col("promised_date")).cast("int")))
      .withColumn("transit_days", when(delivered, datediff(col("delivery_date"), col("ship_date"))))
    def onTimePct: Column = round(avg(col("on_time")) * 100, 2).as("on_time_pct")
    Map(
      "monthly_delivery" -> facts.groupBy("ship_month").agg(
        count("*").as("bookings"), count(when(delivered, 1)).as("delivered"),
        count(when(col("shipment_status") === "CANCELLED", 1)).as("cancelled"),
        count(when(col("shipment_status") === "RETURNED", 1)).as("returned"),
        onTimePct, round(avg(col("transit_days")), 2).as("avg_transit_days")),
      "carrier_scorecard" -> facts.filter(delivered).groupBy("carrier_id", "carrier_name", "sla_on_time_pct")
        .agg(count("*").as("delivered"), onTimePct)
        .withColumn("sla_gap_pp", col("on_time_pct") - col("sla_on_time_pct")),
      "lane_performance" -> facts.filter(eligible)
        .groupBy("origin_hub_id", "origin_hub", "destination_hub_id", "destination_hub")
        .agg(count("*").as("shipments"), sum("freight_charge_inr").as("revenue_inr"),
          round(sum("freight_charge_inr") / sum("weight_kg"), 2).as("revenue_per_kg"), onTimePct),
      "delay_mix" -> facts.filter(col("on_time") === 0)
        .withColumn("delay_reason", when(present("delay_reason"), col("delay_reason")).otherwise("UNSPECIFIED"))
        .groupBy("ship_month", "carrier_id", "delay_reason").count().withColumnRenamed("count", "late_shipments"),
      "segment_revenue" -> facts.filter(eligible).groupBy("ship_month", "segment")
        .agg(sum("freight_charge_inr").as("revenue_inr"), round(avg("weight_kg"), 2).as("avg_weight_kg"),
          round(avg((col("shipment_status") === "RETURNED").cast("int")) * 100, 2).as("return_pct"))
    )
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2 && args.length <= 3,
      "Usage: BatchJob <raw-root> <delta-output-root|--check> [iceberg-catalog.namespace]")
    val spark = SparkSession.builder().appName("Meridian batch").getOrCreate()
    try run(spark, args(0), args(1), args.lift(2)) finally spark.stop()
  }

  def run(spark: SparkSession, rawRoot: String, outputRoot: String,
          icebergNamespace: Option[String]): Unit = {
    import spark.implicits._
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val checkOnly = outputRoot == "--check"
    val raw = keys.keys.map(entity => entity -> read(spark, rawRoot, entity)).toMap
    val reference = Seq("hubs", "carriers", "customers").map { entity =>
      entity -> dimensions(entity, raw(entity))
    }.toMap
    val results = reference + ("shipments" -> shipments(raw("shipments"),
      reference("hubs")._1, reference("carriers")._1, reference("customers")._1))
    def delta(frame: DataFrame, path: String): Unit =
      frame.write.format("delta").mode("overwrite").save(s"$outputRoot/$path")

    def deltaExists(path: String): Boolean = {
      try {
        spark.read.format("delta").load(path).schema
        true
      } catch {
        case error: org.apache.spark.sql.AnalysisException if error.getErrorClass == "PATH_NOT_FOUND" => false
      }
    }
    val counts = results.toSeq.sortBy(_._1).map { case (entity, (accepted, rejected)) =>
      val inputCount = raw(entity).count()
      val acceptedCount = accepted.count()
      val rejectedCount = rejected.count()
      require(inputCount == acceptedCount + rejectedCount, s"Unreconciled counts for $entity")
      require(accepted.select(keys(entity)).distinct().count() == acceptedCount,
        s"Duplicate curated keys for $entity")
      if (!checkOnly) {
        delta(raw(entity), s"bronze/$entity")
        val silverPath = s"$outputRoot/silver/$entity"
        if (entity == "shipments" && deltaExists(silverPath)) {
          accepted.createOrReplaceTempView("meridian_current_shipments")
          try spark.sql(
            s"""MERGE INTO delta.`$silverPath` AS target
               |USING meridian_current_shipments AS source
               |ON target.shipment_id = source.shipment_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED THEN INSERT *
               |WHEN NOT MATCHED BY SOURCE THEN DELETE""".stripMargin
          ).collect()
          finally spark.catalog.dropTempView("meridian_current_shipments")
        } else delta(accepted, s"silver/$entity")
        delta(rejected, s"rejected/$entity")
      }
      (entity, inputCount, acceptedCount, rejectedCount)
    }.toDF("entity", "input_rows", "curated_rows", "rejected_rows")
    counts.show(false)
    if (!checkOnly) delta(counts, "audit/batch_counts")
    gold(results("shipments")._1).toSeq.sortBy(_._1).foreach { case (name, frame) =>
      if (checkOnly) {
        println(s"Meridian KPI: $name")
        frame.show(5, truncate = false)
      } else {
        delta(frame, s"gold/$name")
        icebergNamespace.foreach { namespace =>
          val table = s"$namespace.$name"
          if (spark.catalog.tableExists(table)) frame.writeTo(table).overwrite(lit(true))
          else frame.writeTo(table).using("iceberg").create()
        }
      }
    }
  }
}




// COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "meridian")
dbutils.widgets.dropdown("stage_sample", "true", Seq("true", "false"))

val catalog = dbutils.widgets.get("catalog").trim
val schema = dbutils.widgets.get("schema").trim
val lakeRoot = "abfss://adlsgen2jdcontainer@adlsgen2jd.dfs.core.windows.net/meridian"
require(Seq(catalog, schema).forall(_.matches("[A-Za-z_][A-Za-z0-9_]*")), "Set catalog and schema to simple Unity Catalog identifiers")
val namespace = s"$catalog.$schema"
spark.sql(s"CREATE SCHEMA IF NOT EXISTS $namespace MANAGED LOCATION '$lakeRoot/managed'")
spark.sql(s"CREATE EXTERNAL VOLUME IF NOT EXISTS $namespace.source LOCATION '$lakeRoot/source'")
val sourceRoot = s"/Volumes/$catalog/$schema/source"

// COMMAND ----------
if (dbutils.widgets.get("stage_sample") == "true") {
  val inputs = Map(
  "hubs" -> "aHViX2lkLGh1Yl9uYW1lLGNpdHksc3RhdGUscmVnaW9uLGh1Yl90eXBlLGxhdGl0dWRlLGxvbmdpdHVkZSxjYXBhY2l0eV9zaGlwbWVudHNfcGVyX2RheSxhY3RpdmUKSFVCMDEsSHlkZXJhYmFkLEh5ZGVyYWJhZCxUZWxhbmdhbmEsU291dGgsR2F0ZXdheSwxNy4zODUsNzguNDg2NywxODAwLFkKSFVCMDIsTXVtYmFpLE11bWJhaSxNYWhhcmFzaHRyYSxXZXN0LEdhdGV3YXksMTkuMDc2LDcyLjg3NzcsMjQwMCxZCkhVQjAzLERlbGhpLERlbGhpLERlbGhpLE5vcnRoLEdhdGV3YXksMjguNjEzOSw3Ny4yMDksMjYwMCxZCkhVQjAzLERlbGhpLERlbGhpLERlbGhpLE5vcnRoLEdhdGV3YXksMjguNjEzOSw3Ny4yMDksMjYwMCxZCkhVQjA0LEJlbmdhbHVydSxCZW5nYWx1cnUsS2FybmF0YWthLFNvdXRoLEdhdGV3YXksMTIuOTcxNiw3Ny41OTQ2LDIxMDAsWQpIVUIwNSxDaGVubmFpLENoZW5uYWksVGFtaWwgTmFkdSxTb3V0aCxHYXRld2F5LDEzLjA4MjcsODAuMjcwNywxNzAwLFkKSFVCMDYsS29sa2F0YSxLb2xrYXRhLFdlc3QgQmVuZ2FsLEVhc3QsR2F0ZXdheSwyMi41NzI2LDg4LjM2MzksMTUwMCxZCkhVQjA3LFB1bmUsUHVuZSxNYWhhcmFzaHRyYSxXZXN0LFJlZ2lvbmFsLDE4LjUyMDQsNzMuODU2Nyw5MDAsWQpIVUIwOCxBaG1lZGFiYWQsQWhtZWRhYmFkLEd1amFyYXQsV2VzdCxSZWdpb25hbCwyMy4wMjI1LDcyLjU3MTQsMTAwMCxZCkhVQjA5LEphaXB1cixKYWlwdXIsUmFqYXN0aGFuLE5vcnRoLFJlZ2lvbmFsLDI2LjkxMjQsNzUuNzg3Myw3MDAsWQpIVUIxMCxMdWNrbm93LEx1Y2tub3csVXR0YXIgUHJhZGVzaCxOb3J0aCxSZWdpb25hbCwyNi44NDY3LDgwLjk0NjIsNzUwLFkKSFVCMTEsTmFncHVyLE5hZ3B1cixNYWhhcmFzaHRyYSxDZW50cmFsLFJlZ2lvbmFsLDIxLjE0NTgsNzkuMDg4Miw2NTAsWQpIVUIxMixCaG9wYWwsQmhvcGFsLE1hZGh5YSBQcmFkZXNoLENlbnRyYWwsUmVnaW9uYWwsMjMuMjU5OSw3Ny40MTI2LDUwMCxZCkhVQjEzLEluZG9yZSxJbmRvcmUsTWFkaHlhIFByYWRlc2gsQ2VudHJhbCxSZWdpb25hbCwyMi43MTk2LDc1Ljg1NzcsNjAwLFkKSFVCMTQsVmlzYWtoYXBhdG5hbSxWaXNha2hhcGF0bmFtLEFuZGhyYSBQcmFkZXNoLFNvdXRoLFJlZ2lvbmFsLDE3LjY4NjgsODMuMjE4NSw1NTAsWQpIVUIxNSxLb2NoaSxLb2NoaSxLZXJhbGEsU291dGgsUmVnaW9uYWwsOTkuOTMxNCw3Ni4yNjczLDUwMCxZCkhVQjE2LENvaW1iYXRvcmUsQ29pbWJhdG9yZSxUYW1pbCBOYWR1LFNvdXRoLFJlZ2lvbmFsLDExLjAxNjgsNzYuOTU1OCw0NTAsWQpIVUIxNyxQYXRuYSxQYXRuYSxCaWhhcixFYXN0LFJlZ2lvbmFsLDI1LjU5NDEsODUuMTM3Niw0NTAsWQpIVUIxOCxCaHViYW5lc3dhcixCaHViYW5lc3dhcixPZGlzaGEsRWFzdCxSZWdpb25hbCwyMC4yOTYxLDg1LjgyNDUsNDAwLFkKSFVCMTksR3V3YWhhdGksR3V3YWhhdGksQXNzYW0sTm9ydGhlYXN0LFJlZ2lvbmFsLDI2LjE0NDUsOTEuNzM2MiwzNTAsWQpIVUIyMCxDaGFuZGlnYXJoLENoYW5kaWdhcmgsQ2hhbmRpZ2FyaCxOb3J0aCxMb2NhbCwzMC43MzMzLDc2Ljc3OTQsMzUwLE4KSFVCMjEsU3VyYXQsU3VyYXQsR3VqYXJhdCxXZXN0LExvY2FsLDIxLjE3MDIsNzIuODMxMSw1MDAsWQpIVUIyMixWaWpheWF3YWRhLFZpamF5YXdhZGEsQW5kaHJhIFByYWRlc2gsU291dGgsTG9jYWwsMTYuNTA2Miw4MC42NDgsMzgwLFkKSFVCMjMsV2FyYW5nYWwsV2FyYW5nYWwsVGVsYW5nYW5hLFNvdXRoLExvY2FsLDE3Ljk2ODksNzkuNTk0MSwyNTAsWQpIVUIyNCxNYW5nYWx1cnUsTWFuZ2FsdXJ1LEthcm5hdGFrYSxTb3V0aCxMb2NhbCwxMi45MTQxLDc0Ljg1NiwyODAsWQo=",
  "carriers" -> "Y2Fycmllcl9pZCxjYXJyaWVyX25hbWUsdHJhbnNwb3J0X21vZGUsc2VydmljZV9sZXZlbCxyYXRlX3Blcl9rZ19rbV9pbnIsc2xhX29uX3RpbWVfcGN0LGFjdGl2ZQpDQVIwMDEsU3dpZnRIYXVsIFJvYWR3YXlzLFJvYWQsU3RhbmRhcmQsMC4wMTE1LDkwLFkKQ0FSMDAyLFRyYW5zSW5kaWEgRXhwcmVzcyxSb2FkLEV4cHJlc3MsMC4wMTUsOTQsWQpDQVIwMDMsRGVjY2FuIEZyZWlnaHQgTGluZXMsUm9hZCxTdGFuZGFyZCwwLjAxMDUsODgsWQpDQVIwMDQsQWVyb0NhcmdvIEluZGlhLEFpcixFeHByZXNzLDAuMDYyLDk2LFkKQ0FSMDA1LFJhaWxMaW5rIExvZ2lzdGljcyxSYWlsLEVjb25vbXksMC4wMDc1LDg1LFkKQ0FSMDA2LEtvbmthbiBDYXJyaWVycyxSb2FkLEVjb25vbXksMC4wMDksODIsWQpDQVIwMDYsS29ua2FuIENhcnJpZXJzLFJvYWQsRWNvbm9teSwwLjAwOSw4MixZCkNBUjAwNyxOb3J0aFN0YXIgQ2FyZ28sUm9hZCxTdGFuZGFyZCwwLjAxMiw5MCxZCkNBUjAwOCxHYW5nYSBNb3ZlcnMsUmFpbCxTdGFuZGFyZCwwLjAwODUsODYsWQpDQVIwMDksQ29hc3RhbCBFeHByZXNzIExpbmVzLFJvYWQsRXhwcmVzcywwLjAxNDUsOTMsWQpDQVIwMTAsVmF5dSBBaXIgRnJlaWdodCxBaXIsRXhwcmVzcywwLjA2OCw5NyxZCkNBUjAxMSxNZXRybyBQYXJjZWwgU2VydmljZXMsUm9hZCxFeHByZXNzLDAuMDE2LDkyLFkKQ0FSMDEyLEthdmVyaSBUcmFuc3BvcnQgQ28sUm9hZCxFY29ub215LDAuMDA4OCw4MCxOCg==",
  "customers" -> "Y3VzdG9tZXJfaWQsY3VzdG9tZXJfbmFtZSxlbWFpbCxjaXR5LHNlZ21lbnQsc2lnbnVwX2RhdGUKQ1VTVDAwMDksVmVybWEgTW9iaWxlcyx2ZXJtYS5tb2JpbGVzOEBjb3JwZXhhbXBsZS5pbixNdW1iYWksRW50ZXJwcmlzZSwyMDIzLTEwLTI0CkNVU1QwMDEzLEl5ZXIgUmV0YWlsLGl5ZXIucmV0YWlsMTJAY29ycGV4YW1wbGUuaW4sSHlkZXJhYmFkLEUtY29tbWVyY2UsMjAyNC0wOS0wNQpDVVNUMDAxOCxLdWxrYXJuaSBSZXRhaWwsa3Vsa2FybmkucmV0YWlsMTdAY29ycGV4YW1wbGUuaW4sTXVtYmFpLEVudGVycHJpc2UsMjAyMy0wNy0wOApDVVNUMDAyOCxBYXJhdiBKb3NoaSxhYXJhdi5qb3NoaTI3QG1haWxleGFtcGxlLmluLEJob3BhbCxSZXRhaWwsMjAyMy0wNy0yOQpDVVNUMDAyOCxBYXJhdiBKb3NoaSxhYXJhdi5qb3NoaTI3QG1haWxleGFtcGxlLmluLEJob3BhbCxSZXRhaWwsMjAyMy0wOC0yOApDVVNUMDAzMCxBYWRoeWEgUGF0ZWwsYWFkaHlhLnBhdGVsMjlAbWFpbGV4YW1wbGUuaW4sRGVsaGksUmV0YWlsLDIwMjUtMDEtMDEKQ1VTVDAwMzEsU2FhbnZpIFBpbGxhaSxzYWFudmkucGlsbGFpMzBAbWFpbGV4YW1wbGUuaW4sRGVsaGksUmV0YWlsLDIwMjUtMDEtMTMKQ1VTVDAwMzIsTmVoYSBWZXJtYSxuZWhhLnZlcm1hMzFAbWFpbGV4YW1wbGUuaW4sTXVtYmFpLFJldGFpbCwyMDI0LTExLTEwCkNVU1QwMDM5LFZlcm1hIEZvb2RzLHZlcm1hLmZvb2RzMzhAY29ycGV4YW1wbGUuaW4sQmVuZ2FsdXJ1LFNNRSwyMDI1LTAxLTE0CkNVU1QwMDQyLFJhbyBNb2JpbGVzLHJhby5tb2JpbGVzNDFAY29ycGV4YW1wbGUuaW4sS29jaGksRW50ZXJwcmlzZSwyMDI0LTA2LTA1CkNVU1QwMDUwLE5haWR1IEF1dG8gUGFydHMsbmFpZHUuYXV0by5wYXJ0czQ5QGNvcnBleGFtcGxlLmluLGx1Y2tub3csU01FLDIwMjQtMDEtMzEKQ1VTVDAwNTIsSGFyc2hhIENoYXR0ZXJqZWUsaGFyc2hhLmNoYXR0ZXJqZWU1MUBtYWlsZXhhbXBsZS5pbixNdW1iYWksUmV0YWlsLDIwMjUtMDktMTcKQ1VTVDAwNTgsU2luZ2ggUmV0YWlsLHNpbmdoLnJldGFpbDU3QGNvcnBleGFtcGxlLmluLE11bWJhaSxTTUUsMjAyNS0wMS0wNQpDVVNUMDA2NixOYWlkdSBGb29kcyxuYWlkdS5mb29kczY1QGNvcnBleGFtcGxlLmluLEJodWJhbmVzd2FyLEVudGVycHJpc2UsMjAyNC0wNi0yOApDVVNUMDA2NixOYWlkdSBGb29kcyxuYWlkdS5mb29kczY1QGNvcnBleGFtcGxlLmluLEJodWJhbmVzd2FyLEVudGVycHJpc2UsMjAyNC0wNy0yOApDVVNUMDA3MixOYWlkdSBGdXJuaXNoaW5ncyxuYWlkdS5mdXJuaXNoaW5nczcxQGNvcnBleGFtcGxlLmluLEh5ZGVyYWJhZCxTTUUsMjAyMy0wNC0wNApDVVNUMDA3NixWZXJtYSBFbnRlcnByaXNlcyx2ZXJtYS5lbnRlcnByaXNlczc1QGNvcnBleGFtcGxlLmluLFZpc2FraGFwYXRuYW0sU01FLDIwMjQtMTAtMjYKQ1VTVDAwNzcsTWVodGEgQ2hlbWljYWxzLG1laHRhLmNoZW1pY2Fsczc2QGNvcnBleGFtcGxlLmluLE11bWJhaSxFLWNvbW1lcmNlLDIwMjQtMTItMDYKQ1VTVDAwNzksSXllciBFeHBvcnRzLGl5ZXIuZXhwb3J0czc4QGNvcnBleGFtcGxlLmluLENoZW5uYWksU01FLDIwMjMtMDMtMDkKQ1VTVDAwODQsUGlsbGFpIEZvb2RzLHBpbGxhaS5mb29kczgzQGNvcnBleGFtcGxlLmluLEphaXB1cixFLWNvbW1lcmNlLDIwMjMtMTEtMTcKQ1VTVDAwOTIsUGlsbGFpIEFncm8scGlsbGFpLmFncm85MUBjb3JwZXhhbXBsZS5pbixDaGVubmFpLFNNRSwyMDI0LTA2LTA4CkNVU1QwMDkzLFJhbWVzaCBWZXJtYSxyYW1lc2gudmVybWE5MkBtYWlsZXhhbXBsZS5pbixLb2xrYXRhLFJldGFpbCwyMDI1LTAyLTI1CkNVU1QwMTAwLEd1cHRhIENoZW1pY2FscyxndXB0YS5jaGVtaWNhbHM5OUBjb3JwZXhhbXBsZS5pbixEZWxoaSxTTUUsMjAyNC0xMC0wOQpDVVNUMDEwMCxHdXB0YSBDaGVtaWNhbHMsZ3VwdGEuY2hlbWljYWxzOTlAY29ycGV4YW1wbGUuaW4sRGVsaGksU01FLDIwMjQtMTEtMDgKQ1VTVDAxMDcsU2hhcm1hIEVudGVycHJpc2VzLHNoYXJtYS5lbnRlcnByaXNlczEwNkBjb3JwZXhhbXBsZS5pbixOYWdwdXIsRS1jb21tZXJjZSwyMDI1LTA3LTA4CkNVU1QwMTE5LFNuZWhhIEd1cHRhLHNuZWhhLmd1cHRhMTE4QG1haWxleGFtcGxlLmluLGh5ZGVyYWJhZCxSZXRhaWwsMjAyNC0xMS0xOApDVVNUMDEyOCxHdXB0YSBUcmFkZXJzLGd1cHRhLnRyYWRlcnMxMjdAY29ycGV4YW1wbGUuaW4sTmFncHVyLEUtY29tbWVyY2UsMjAyNS0wOC0yMwpDVVNUMDEzNCxTaGFybWEgSW5kdXN0cmllcyxzaGFybWEuaW5kdXN0cmllczEzM0Bjb3JwZXhhbXBsZS5pbixBaG1lZGFiYWQsU01FLDIwMjUtMDItMTgKQ1VTVDAxMzUsQm9zZSBFeHBvcnRzLGJvc2UuZXhwb3J0czEzNEBjb3JwZXhhbXBsZS5pbixCZW5nYWx1cnUsU01FLDIwMjQtMDEtMTAKQ1VTVDAxNDIsS2FydGhpayBHdXB0YSxrYXJ0aGlrLmd1cHRhMTQxQG1haWxleGFtcGxlLmluLEFobWVkYWJhZCxSZXRhaWwsMjAyMy0xMi0xMgpDVVNUMDE0NCxNZWh0YSBFbGVjdHJvbmljcyxtZWh0YS5lbGVjdHJvbmljczE0M0Bjb3JwZXhhbXBsZS5pbixLb2NoaSxTTUUsMjAyMy0wOC0wMgpDVVNUMDE0OCxCb3NlIEVsZWN0cm9uaWNzLGJvc2UuZWxlY3Ryb25pY3MxNDdAY29ycGV4YW1wbGUuaW4sRGVsaGksRW50ZXJwcmlzZSwyMDI0LTA0LTE1CkNVU1QwMTU0LFNpbmdoIEV4cG9ydHMsc2luZ2guZXhwb3J0czE1M0Bjb3JwZXhhbXBsZS5pbixBaG1lZGFiYWQsRS1jb21tZXJjZSwyMDIzLTA1LTIzCkNVU1QwMTU5LEFhcmF2IEpvc2hpLGFhcmF2Lmpvc2hpMTU4QG1haWxleGFtcGxlLmluLERlbGhpLFJldGFpbCwyMDI1LTA3LTA1CkNVU1QwMTYwLEd1cHRhIFBoYXJtYSxndXB0YS5waGFybWExNTlAY29ycGV4YW1wbGUuaW4sSW5kb3JlLEUtY29tbWVyY2UsMjAyMy0wOC0xOQpDVVNUMDE2OSxNZWh0YSBBdXRvIFBhcnRzLG1laHRhLmF1dG8ucGFydHMxNjhAY29ycGV4YW1wbGUuaW4sTXVtYmFpLEVudGVycHJpc2UsMjAyMy0xMC0xMwpDVVNUMDE4OCxBYXJhdiBEYXMsYWFyYXYuZGFzMTg3QG1haWxleGFtcGxlLmluLFZpamF5YXdhZGEsUmV0YWlsLDIwMjUtMTAtMDMKQ1VTVDAxODksSXllciBFbnRlcnByaXNlcyxpeWVyLmVudGVycHJpc2VzMTg4QGNvcnBleGFtcGxlLmluLENoZW5uYWksRW50ZXJwcmlzZSwyMDIzLTA2LTE1CkNVU1QwMTk1LFZpdmFhbiBLdWxrYXJuaSx2aXZhYW4ua3Vsa2FybmkxOTRAbWFpbGV4YW1wbGUuaW4sQ29pbWJhdG9yZSxSZXRhaWwsMjAyNS0xMS0wMQpDVVNUMDE5OSxJeWVyIEF1dG8gUGFydHMsaXllci5hdXRvLnBhcnRzMTk4QGNvcnBleGFtcGxlLmluLFBhdG5hLFNNRSwyMDIzLTAxLTI5CkNVU1QwMjAxLEJvc2UgQXV0byBQYXJ0cyxib3NlLmF1dG8ucGFydHMyMDBAY29ycGV4YW1wbGUuaW4sTHVja25vdyxTTUUsMjAyMy0wNy0zMApDVVNUMDIwNSxNZW5vbiBDaGVtaWNhbHMsbWVub24uY2hlbWljYWxzMjA0QGNvcnBleGFtcGxlLmluLENoZW5uYWksU01FLDIwMjUtMDMtMzAKQ1VTVDAyMDcsS2hhbiBSZXRhaWwsa2hhbi5yZXRhaWwyMDZAY29ycGV4YW1wbGUuaW4sSHlkZXJhYmFkLEUtY29tbWVyY2UsMjAyMy0wNi0yNgpDVVNUMDIwOCxJeWVyIFJldGFpbCxpeWVyLnJldGFpbDIwN0Bjb3JwZXhhbXBsZS5pbixLb2xrYXRhLEUtY29tbWVyY2UsMjAyNS0wOS0yNApDVVNUMDIzNyxBcmp1biBQaWxsYWksYXJqdW4ucGlsbGFpMjM2QG1haWxleGFtcGxlLmluLFdhcmFuZ2FsLFJldGFpbCwyMDI1LTA0LTAzCkNVU1QwMjUzLFN1bml0YSBNZWh0YSxzdW5pdGEubWVodGEyNTJAbWFpbGV4YW1wbGUuaW4sQ2hlbm5haSxSZXRhaWwsMjAyMy0xMS0xNwpDVVNUMDI1NCxLaGFuIENlcmFtaWNzLGtoYW4uY2VyYW1pY3MyNTNAY29ycGV4YW1wbGUuaW4sTmFncHVyLFNNRSwyMDIzLTExLTIxCkNVU1QwMjYxLEd1cHRhIEF1dG8gUGFydHMsZ3VwdGEuYXV0by5wYXJ0czI2MEBjb3JwZXhhbXBsZS5pbixDb2ltYmF0b3JlLFNNRSwyMDIzLTA1LTI5CkNVU1QwMjYzLFZlcm1hIENoZW1pY2Fscyx2ZXJtYS5jaGVtaWNhbHMyNjJAY29ycGV4YW1wbGUuaW4sQmhvcGFsLEUtY29tbWVyY2UsMjAyNS0xMS0xMwpDVVNUMDI4MCxQYXRlbCBJbmR1c3RyaWVzLHBhdGVsLmluZHVzdHJpZXMyNzlAY29ycGV4YW1wbGUuaW4sTXVtYmFpLEVudGVycHJpc2UsMjAyMy0wNi0yMwpDVVNUMDI4NSxTaW5naCBJbmR1c3RyaWVzLHNpbmdoLmluZHVzdHJpZXMyODRAY29ycGV4YW1wbGUuaW4sQmhvcGFsLFNNRSwyMDI1LTA0LTI0CkNVU1QwMjg2LERhcyBSZXRhaWwsZGFzLnJldGFpbDI4NUBjb3JwZXhhbXBsZS5pbixNdW1iYWksRS1jb21tZXJjZSwyMDI1LTAyLTI3CkNVU1QwMjk3LENoYXR0ZXJqZWUgSW5kdXN0cmllcyxjaGF0dGVyamVlLmluZHVzdHJpZXMyOTZAY29ycGV4YW1wbGUuaW4sSmFpcHVyLEUtY29tbWVyY2UsMjAyMy0wOS0wOQpDVVNUMDMwMCxTaGFybWEgQ2VyYW1pY3Msc2hhcm1hLmNlcmFtaWNzMjk5QGNvcnBleGFtcGxlLmluLEtvbGthdGEsRS1jb21tZXJjZSwyMDI1LTA2LTI3CkNVU1QwMzE3LE5haWR1IENoZW1pY2FscyxuYWlkdS5jaGVtaWNhbHMzMTZAY29ycGV4YW1wbGUuaW4sQmVuZ2FsdXJ1LEUtY29tbWVyY2UsMjAyNS0wNi0yNgpDVVNUMDMxOSxBZGl0eWEgUGF0ZWwsYWRpdHlhLnBhdGVsMzE4QG1haWxleGFtcGxlLmluLENvaW1iYXRvcmUsUmV0YWlsLDIwMjMtMDktMTgKQ1VTVDAzMjIsRGFzIFJldGFpbCxkYXMucmV0YWlsMzIxQGNvcnBleGFtcGxlLmluLFB1bmUsRS1jb21tZXJjZSwyMDI1LTAyLTIyCkNVU1QwMzI3LEtoYW4gQ2hlbWljYWxzLGtoYW4uY2hlbWljYWxzMzI2QGNvcnBleGFtcGxlLmluLEphaXB1cixTTUUsMjAyMy0xMi0xOQpDVVNUMDMzMCxHdXB0YSBFeHBvcnRzLGd1cHRhLmV4cG9ydHMzMjlAY29ycGV4YW1wbGUuaW4sQmVuZ2FsdXJ1LFNNRSwyMDI0LTEwLTA4CkNVU1QwMzMyLEpvc2hpIE1vYmlsZXMsam9zaGkubW9iaWxlczMzMUBjb3JwZXhhbXBsZS5pbixLb2xrYXRhLFNNRSwyMDIzLTA1LTExCkNVU1QwMzM3LFN1cmVzaCBQaWxsYWksc3VyZXNoLnBpbGxhaTMzNkBtYWlsZXhhbXBsZS5pbixCZW5nYWx1cnUsUmV0YWlsLDIwMjUtMDctMDQKQ1VTVDAzNDEsS2hhbiBSZXRhaWwsa2hhbi5yZXRhaWwzNDBAY29ycGV4YW1wbGUuaW4sQmVuZ2FsdXJ1LEUtY29tbWVyY2UsMjAyNS0xMS0yOApDVVNUMDM0MyxNZWh0YSBJbmR1c3RyaWVzLG1laHRhLmluZHVzdHJpZXMzNDJAY29ycGV4YW1wbGUuaW4sU3VyYXQsU01FLDIwMjQtMDItMDkKQ1VTVDAzNDUsTWVodGEgRXhwb3J0cyxtZWh0YS5leHBvcnRzMzQ0QGNvcnBleGFtcGxlLmluLFB1bmUsRW50ZXJwcmlzZSwyMDI1LTA0LTEzCkNVU1QwMzQ3LENoYXR0ZXJqZWUgTW9iaWxlcyxjaGF0dGVyamVlLm1vYmlsZXMzNDZAY29ycGV4YW1wbGUuaW4sUGF0bmEsRW50ZXJwcmlzZSwyMDI0LTA2LTAyCkNVU1QwMzQ4LEl5ZXIgUmV0YWlsLGl5ZXIucmV0YWlsMzQ3QGNvcnBleGFtcGxlLmluLENIRU5OQUksU01FLDIwMjMtMDUtMDcKQ1VTVDAzNDksS2FydGhpayBQaWxsYWksa2FydGhpay5waWxsYWkzNDhAbWFpbGV4YW1wbGUuaW4sRGVsaGksUmV0YWlsLDIwMjUtMDYtMTkKQ1VTVDAzNTUsSXllciBBdXRvIFBhcnRzLGl5ZXIuYXV0by5wYXJ0czM1NEBjb3JwZXhhbXBsZS5pbixIeWRlcmFiYWQsU01FLDIwMjUtMDktMDYKQ1VTVDAzNTgsU2luZ2ggVGV4dGlsZXMsc2luZ2gudGV4dGlsZXMzNTdAY29ycGV4YW1wbGUuaW4sRGVsaGksRW50ZXJwcmlzZSwyMDI1LTAzLTA5CkNVU1QwMzYzLE1laHRhIEZvb2RzLG1laHRhLmZvb2RzMzYyQGNvcnBleGFtcGxlLmluLE11bWJhaSxTTUUsMjAyNC0xMS0wNApDVVNUMDM2NixQaWxsYWkgQXV0byBQYXJ0cyxwaWxsYWkuYXV0by5wYXJ0czM2NUBjb3JwZXhhbXBsZS5pbixIeWRlcmFiYWQsU01FLDIwMjQtMDYtMTgKQ1VTVDAzNzcsTWVodGEgQWdybyxtZWh0YS5hZ3JvMzc2QGNvcnBleGFtcGxlLmluLEphaXB1cixTTUUsMjAyNS0wMi0xNwpDVVNUMDM3OCxKb3NoaSBFbnRlcnByaXNlcyxqb3NoaS5lbnRlcnByaXNlczM3N0Bjb3JwZXhhbXBsZS5pbixDaGFuZGlnYXJoLFNNRSwyMDI0LTA4LTIzCkNVU1QwMzgyLEFyanVuIEtoYW4sYXJqdW4ua2hhbjM4MUBtYWlsZXhhbXBsZS5pbixQdW5lLFJldGFpbCwyMDI0LTExLTMwCkNVU1QwMzg3LFBvb2phIE1lbm9uLHBvb2phLm1lbm9uMzg2QG1haWxleGFtcGxlLmluLE11bWJhaSxSZXRhaWwsMjAyMy0wMy0wOQpDVVNUMDM4OCxCb3NlIEF1dG8gUGFydHMsYm9zZS5hdXRvLnBhcnRzMzg3QGNvcnBleGFtcGxlLmluLEJlbmdhbHVydSxTTUUsMjAyMy0wNS0wMgpDVVNUMDM5MSxTaGFybWEgUmV0YWlsLHNoYXJtYS5yZXRhaWwzOTBAY29ycGV4YW1wbGUuaW4sSHlkZXJhYmFkLEVudGVycHJpc2UsMjAyNC0wMi0wMQpDVVNUMDM5MixNZW5vbiBUcmFkZXJzLG1lbm9uLnRyYWRlcnMzOTFAY29ycGV4YW1wbGUuaW4sQmVuZ2FsdXJ1LEVudGVycHJpc2UsMjAyMy0wMy0xMQo=",
  "shipments" -> "c2hpcG1lbnRfaWQsY3VzdG9tZXJfaWQsY2Fycmllcl9pZCxvcmlnaW5faHViX2lkLGRlc3RpbmF0aW9uX2h1Yl9pZCxzaGlwX2RhdGUscHJvbWlzZWRfZGF0ZSxkZWxpdmVyeV9kYXRlLHdlaWdodF9rZyxkaXN0YW5jZV9rbSxmcmVpZ2h0X2NoYXJnZV9pbnIsc2hpcG1lbnRfc3RhdHVzLGRlbGF5X3JlYXNvbixsYXN0X3VwZGF0ZWQKU0hQMjAyNjAwMDA2OCwsQ0FSMDA0LEhVQjA4LEhVQjAxLDIwMjYtMDEtMDIsMjAyNi0wMS0wMywyMDI2LTAxLTAzLDIwLjEsMTA5OS40LDE0NDkuNzgsREVMSVZFUkVELCwyMDI2LTAxLTAzIDA2OjA2OjAwClNIUDIwMjYwMDAwNzUsQ1VTVDAwMzIsQ0FSMDA4LEhVQjEwLEhVQjA2LDIwMjYtMDEtMDMsMjAyNi0wMS0wNiwyMDI2LTAxLTA2LDUuNSwxMTA4LjcsMTgwLjAsZGVsaXZlcmVkLCwyMDI2LTAxLTA2IDE5OjQ4OjAwClNIUDIwMjYwMDAwNzYsQ1VTVDAzNjMsQ0FSMDAzLEhVQjA2LEhVQjEwLDIwMjYtMDEtMDMsMjAyNi0wMS0wNywyMDI2LTAxLTEwLC0xMi41LDExMDguNywyMjg2Ljk1LERFTElWRVJFRCxWZWhpY2xlIEJyZWFrZG93biwyMDI2LTAxLTEwIDIwOjMxOjAwClNIUDIwMjYwMDAwNzcsQ1VTVDAyOTcsQ0FSMDAxLEhVQjA2LEhVQjAyLDAzLTAxLTIwMjYsMjAyNi0wMS0wOSwyMDI2LTAxLTA5LDc5LjcsMjA2OC41LDE3NzQuMTEsUkVUVVJORUQsLDIwMjYtMDEtMDkgMTE6Mjc6MDAKU0hQMjAyNjAwMDA5OSxDVVNUMDI2MSxDQVIwMDQsSFVCMDIsSFVCMDQsMjAyNi0wMS0wNCwyMDI2LTAxLTA1LDIwMjYtMDEtMDUsMCwxMDU2LjYsOTMxMS41MixERUxJVkVSRUQsLDIwMjYtMDEtMDUgMTY6MTA6MDAKU0hQMjAyNjAwMDEwMSxDVVNUMDM0NyxDQVIwMDIsSFVCMDUsSFVCMjEsMjAyNi0wMS0wNCwyMDI2LTAxLTA3LDIwMjYtMDEtMDYsMzc3LjUsMTQ5Ni4wLDg1NzUuNDcsREVMSVZFUkVELCwyMDI2LTAxLTA2IDEzOjEyOjAwClNIUDIwMjYwMDAxMDEsQ1VTVDAzNDcsQ0FSMDAyLEhVQjA1LEhVQjIxLDIwMjYtMDEtMDQsMjAyNi0wMS0wNywyMDI2LTAxLTA2LDM3Ny41LDE0OTYuMCw4NTc1LjQ3LERFTElWRVJFRCwsMjAyNi0wMS0wNiAxMzoxMjowMApTSFAyMDI2MDAwMTEzLCxDQVIwMTEsSFVCMDMsSFVCMDcsMjAyNi0wMS0wNSwyMDI2LTAxLTA4LDIwMjYtMDEtMDgsMTcuNywxNDY2LjIsNDIzLjU2LERFTElWRVJFRCwsMjAyNi0wMS0wOCAxNzoxMjowMApTSFAyMDI2MDAwMTUyLENVU1QwMzMwLENBUjAwOSxIVUIwNCxIVUIwMywyMDI2LTAxLTA2LDIwMjYtMDEtMTEsMjAyNi0wMS0xMSw4NC43LDIxNzQuOCwyNTE0LjYyLERFTElWRVJFRCwsMjAyNi0wMS0xMSAxMDo0MDowMApTSFAyMDI2MDAwMTUyLENVU1QwMzMwLENBUjAwOSxIVUIwNCxIVUIwMywyMDI2LTAxLTA2LDIwMjYtMDEtMTEsMjAyNi0wMS0xMSw4NC43LDIxNzQuOCwyNTE0LjYyLERFTElWRVJFRCwsMjAyNi0wMS0xMSAxMDo0MDowMApTSFAyMDI2MDAwMTU3LENVU1Q5MDAyLENBUjAwNCxIVUIwMyxIVUIyMywyMDI2LTAxLTA3LDIwMjYtMDEtMDgsMjAyNi0wMS0wOCw0Ni45LDE1MTAuNSw0NjQ5LjQ0LERFTElWRVJFRCwsMjAyNi0wMS0wOCAxNzo1NjowMApTSFAyMDI2MDAwMjMzLENVU1QwMDc5LENBUjAwOCxIVUIwMyxIVUIxOCwyMDI2LTAxLTA5LDIwMjYtMDEtMTMsMjAyNi0wMS0xNCwsMTU4Ny44LDEwNTIuNTMsREVMSVZFUkVELFZlaGljbGUgQnJlYWtkb3duLDIwMjYtMDEtMTQgMDg6MTA6MDAKU0hQMjAyNjAwMDMyOCxDVVNUMDM0MSxDQVIwMDMsSFVCMDEsSFVCMjQsMjAyNi0wMS0xNCwyMDI2LTAxLTE3LDIwMjYtMDEtMTcsMzEuOSw3ODkuNSwyNjkuMTcsREVMSVZFUkVELCwyMDI2LTAxLTE3IDA3OjI3OjAwClNIUDIwMjYwMDAzMjgsQ1VTVDAzNDEsQ0FSMDAzLEhVQjAxLEhVQjI0LDIwMjYtMDEtMTQsMjAyNi0wMS0xNywyMDI2LTAxLTE3LDMxLjksNzg5LjUsMjY5LjE3LERFTElWRVJFRCwsMjAyNi0wMS0xNyAwNzoyNzowMApTSFAyMDI2MDAwMzI5LENVU1QwMDc3LENBUjAwNixIVUIwNCxIVUIwMywyMDI2LTAxLTE0LDIwMjYtMDEtMjEsMjAyNi0wMS0yMSwxOC4xLDIxNzQuOCwzNTQuMjQsREVMSVZFUkVELCwyMDI2LTAxLTIxIDA4OjA4OjAwClNIUDIwMjYwMDAzMjksQ1VTVDAwNzcsQ0FSMDA2LEhVQjA0LEhVQjAzLDIwMjYtMDEtMTQsMjAyNi0wMS0yMSwyMDI2LTAxLTIxLDE4LjEsMjE3NC44LDM1NC4yNCxERUxJVkVSRUQsLDIwMjYtMDEtMjEgMDg6MDg6MDAKU0hQMjAyNjAwMDM1NixDVVNUMDAxMyxDQVIwMDUsSFVCMDgsSFVCMDIsMjAyNi0wMS0xNSwyMDI2LTAxLTE4LDIwMjYtMDEtMTgsMTAuMiw1NTAuMCwxODAuMCxERUxJVkVSRUQsLDIwMjYtMDEtMTggMDg6MzM6MDAKU0hQMjAyNjAwMDM3NyxDVVNUMDE5OSxDQVIwMDQsSFVCMDIsSFVCMTIsMjAyNi0wMS0xNSwyMDI2LTAxLTE2LDIwMjYtMDEtMTYsOTcuMyw4MjYuNyw1MjI0LjEsREVMSVZFUkVELCwyMDI2LTAxLTE2IDE1OjQ3OjAwClNIUDIwMjYwMDA0MjMsQ1VTVDAwMzEsQ0FSMDEwLEhVQjE5LEhVQjAyLDIwMjYtMDEtMTcsMjAyNi0wMS0xOSwyMDI2LTAxLTE5LDIwLjksMjYwNy45LDM3NTcuMjUsREVMSVZFUkVELCwyMDI2LTAxLTE5IDA2OjQ2OjAwClNIUDIwMjYwMDA0NDEsQ1VTVDAzMzIsQ0FSMDEwLEhVQjAzLEhVQjA0LDIwMjYtMDEtMTksMjAyNi0wMS0yMSwyMDI2LTAxLTIxLDE5MS40LDIxNzQuOCwyOTEwMS4xNyxERUxJVkVSRUQsLDIwMjYtMDEtMjEgMDY6MTg6MDAKU0hQMjAyNjAwMDQ0MSxDVVNUMDMzMixDQVIwMTAsSFVCMDMsSFVCMDQsMjAyNi0wMS0xOSwyMDI2LTAxLTIxLDIwMjYtMDEtMjEsMTkxLjQsMjE3NC44LDI5MTAxLjE3LERFTElWRVJFRCwsMjAyNi0wMS0yMSAwNjoxODowMApTSFAyMDI2MDAwNDk1LENVU1QwMTAwLENBUjAwNixIVUIxNSxIVUIwNCwyMDI2LTAxLTIxLDIwMjYtMDEtMjQsMjAyNi0wMS0yNCwxMjUuOCw0NTkuNiw1NTguODIsREVMSVZFUkVELCwyMDI2LTAxLTI0IDE3OjA1OjAwClNIUDIwMjYwMDA0OTUsQ1VTVDAxMDAsQ0FSMDA2LEhVQjE1LEhVQjA0LDIwMjYtMDEtMjEsMjAyNi0wMS0yNCwyMDI2LTAxLTI0LDEyNS44LDQ1OS42LDU1OC44MixERUxJVkVSRUQsLDIwMjYtMDEtMjQgMTc6MDU6MDAKU0hQMjAyNjAwMDUwNCxDVVNUMDMxOSxDQVIwMDYsSFVCMTYsSFVCMDQsMjAyNi0wMS0yMSwyMDI2LTAxLTI0LDIwMjYtMDEtMjQsNC45LDI4NS4yLDE4MC4wLERFTElWRVJFRCwsMjAyNi0wMS0yNCAxNDoyMjowMApTSFAyMDI2MDAwNTM1LENVU1QwMTI4LENBUjAwNSxIVUIyNCxIVUIxMiwyMDI2LTAxLTIyLDIwMjYtMDEtMjcsMjAyNi0wMS0yOSw3NS4wLDE0NzcuMCw4NTIuNDEsREVMSVZFUkVELFRyYWZmaWMgQ29uZ2VzdGlvbiwyMDI2LTAxLTI5IDE5OjM5OjAwClNIUDIwMjYwMDA1MzUsQ1VTVDAxMjgsQ0FSMDA1LEhVQjI0LEhVQjEyLDIwMjYtMDEtMjIsMjAyNi0wMS0yNywyMDI2LTAxLTI5LDc1LjAsMTQ3Ny4wLDg1Mi40MSxERUxJVkVSRUQsVHJhZmZpYyBDb25nZXN0aW9uLDIwMjYtMDEtMjkgMTk6Mzk6MDAKU0hQMjAyNjAwMDU0NSxDVVNUMDMyNyxDQVIwMDMsSFVCMDcsSFVCMTEsMjAyNi0wMS0yMywyMDI2LTAxLTI2LDIwMjYtMDEtMjYsMjA2LjQsLDE3ODMuMTQsREVMSVZFUkVELCwyMDI2LTAxLTI2IDEzOjIyOjAwClNIUDIwMjYwMDA1NjcsQ1VTVDAxNTQsQ0FSMDA5LEhVQjExLEhVQjIyLDIwMjYtMDEtMjQsMjAyNi0wMS0yNiwyMDI2LTAxLTI2LDYuNSw2NzYuNywxODAuMCxSRVRVUk5FRCwsMjAyNi0wMS0yNiAxODozMDowMApTSFAyMDI2MDAwNTg3LENVU1QwMTM1LENBUjkwNSxIVUIwNCxIVUIyMiwyMDI2LTAxLTI1LDIwMjYtMDEtMjcsMjAyNi0wMS0yNywzNjMuOSw2NDAuMSwzNjI1LjgyLERFTElWRVJFRCwsMjAyNi0wMS0yNyAxNjozMDowMApTSFAyMDI2MDAwNjEwLENVU1QwMzMyLENBUjAxMSxIVUIwNSxIVUIwNCwyMDI2LTAxLTI2LDIwMjYtMDEtMjcsMjAyNi0wMS0yNywxNDMuOCwzNjIuNyw4MTAuMDcsREVMSVZFUkVELCwyMDI2LTAxLTI3IDEwOjMxOjAwClNIUDIwMjYwMDA3MzksQ1VTVDAxMDAsQ0FSMDAyLEhVQjA4LEhVQjA0LDIwMjYtMDEtMzEsMjAyNi0wMi0wNCwyMDI2LTAyLTAzLDEwMy44LDE1NDYuMywyMjQxLjUxLERFTElWRVJFRCwsMjAyNi0wMi0wMyAxNToxNDowMApTSFAyMDI2MDAwNzM5LENVU1QwMTAwLENBUjAwMixIVUIwOCxIVUIwNCwyMDI2LTAxLTMxLDIwMjYtMDItMDQsMjAyNi0wMi0wMywxMDMuOCwxNTQ2LjMsMjI0MS41MSxERUxJVkVSRUQsLDIwMjYtMDItMDMgMTU6MTQ6MDAKU0hQMjAyNjAwMDc1MyxDVVNUMDE0OCxDQVIwMDYsSFVCMDMsSFVCMTgsMDEtMDItMjAyNiwyMDI2LTAyLTA3LDIwMjYtMDItMTAsNDg1LjIsMTU4Ny44LDcwMDQuMjIsREVMSVZFUkVELFdlYXRoZXIsMjAyNi0wMi0xMCAyMTo0MDowMApTSFAyMDI2MDAwNzgyLENVU1QwMzQ1LENBUjAxMSxIVUIwMyxIVUIyMSwyMDI2LTAyLTAyLDIwMjYtMDItMDUsMjAyNi0wMi0wNSw3MzcuMiwxMTcyLjMsMTI5MzkuNTMsUkVUVVJORUQsLDIwMjYtMDItMDUgMTg6NDQ6MDAKU0hQMjAyNjAwMDgyOSxDVVNUMDIwNSxDQVIwMDIsSFVCMTcsSFVCMTMsMDQtMDItMjAyNiwyMDI2LTAyLTA3LDIwMjYtMDItMDcsMTMyLjcsMTI0Mi41LDI0NjUuNTksREVMSVZFUkVELCwyMDI2LTAyLTA3IDEyOjAzOjAwClNIUDIwMjYwMDA4ODgsQ1VTVDAyNjMsQ0FSMDA5LEhVQjAyLEhVQjEwLDIwMjYtMDItMDYsMjAyNi0wMi0wOSwyMDI2LTAyLTA5LDEwLjAsMTQ5My4zLDIyOS4yMyxERUxJVkVSRUQsLDIwMjYtMDItMDkgMjE6Mjk6MDAKU0hQMjAyNjAwMDkyNCxDVVNUMDMzMixDQVIwMDIsSFVCMDUsSFVCMDMsMjAyNi0wMi0wNywyMDI2LTAyLTEyLDIwMjYtMDItMTIsNTMuNCwyMTk0LjcsMTg0My4xOSwgREVMSVZFUkVELCwyMDI2LTAyLTEyIDA2OjA2OjAwClNIUDIwMjYwMDA5MzEsQ1VTVDAyOTcsQ0FSMDA3LEhVQjEzLEhVQjA5LDIwMjYtMDItMDgsMjAyNi0wMi0xMSwyMDI2LTAyLTExLDMzLjAsNTgyLjgsLTIxOS4zNyxERUxJVkVSRUQsLDIwMjYtMDItMTEgMDY6NDg6MDAKU0hQMjAyNjAwMDk3MixDVVNUMDA5MixDQVIwMDgsSFVCMjEsSFVCMDQsMjAyNi0wMi0xMCwyMDI2LTAyLTE0LDIwMjYtMDItMTMsOTMuNSwsMTAxMi43OCxERUxJVkVSRUQsLDIwMjYtMDItMTMgMTk6Mzc6MDAKU0hQMjAyNjAwMTA1NCxDVVNUMDIwNyxDQVIwMDIsSFVCMDcsSFVCMDIsMjAyNi0wMi0xMywyMDI2LTAyLTE0LDIwMjYtMDItMTQsNTAuMCwxNTAuMiwxODAuMCxETFZSRCwsMjAyNi0wMi0xNCAxMzo1ODowMApTSFAyMDI2MDAxMDcxLENVU1QwMzg4LENBUjAwNSxIVUIwNixIVUIwMiwyMDI2LTAyLTE0LDIwMjYtMDItMjAsMjAyNi0wMi0yMCwxNjQuNSwyMDY4LjUsMjY5OC4yNCxERUxJVkVSRUQsLDIwMjYtMDItMjAgMjE6MTQ6MDAKU0hQMjAyNjAwMTE0OCxDVVNUMDAzMCxDQVIwMDEsSFVCMDUsSFVCMDMsMjAyNi0wMi0xNywyMDI2LTAyLTIzLDIwMjYtMDItMjMsMTEuNywyMTk0LjcsMzE3Ljk2LGRlbGl2ZXJlZCwsMjAyNi0wMi0yMyAwNzoyNzowMApTSFAyMDI2MDAxMTcwLENVU1QwMDc3LENBUjkwOCxIVUIxOSxIVUIxOCwyMDI2LTAyLTE4LDIwMjYtMDItMTksLDM5LjksMTEwOS4xLDI3NzAuMDEsQ0FOQ0VMTEVELCwyMDI2LTAyLTE4IDIxOjAwOjAwClNIUDIwMjYwMDExODYsQ1VTVDAzNDksQ0FSMDAyLEhVQjA3LEhVQjAzLDIwMjYtMDItMTksMjAyNi0wMi0yMiwyMDI2LTAyLTIyLDIwLjAsMTQ2Ni4yLDQxNy42NCxERUxJVkVSRUQsLDIwMjYtMDItMjIgMjI6MTg6MDAKU0hQMjAyNjAwMTE4OSxDVVNUMDEwMCxDQVIwMDIsSFVCMDIsSFVCMDMsMjAyNi0wMi0xOSwyMDI2LTAyLTIyLDIwMjYtMDItMjEsMTE0LjksMTQzNS4xLDIzODkuOTgsREVMSVZFUkVELCwyMDI2LTAyLTIxIDEwOjEzOjAwClNIUDIwMjYwMDEyNDIsQ1VTVDAzODcsQ0FSMDAzLEhVQjExLEhVQjE4LDIwMjYtMDItMjEsMjAyNi0wMi0yNCwyMDI2LTAyLTI0LDQuMCw4ODMuNiwxODAuMCxETFZSRCwsMjAyNi0wMi0yNCAyMDo1OTowMApTSFAyMDI2MDAxMjkzLENVU1QwMjg2LENBUjAwOCxIVUIxNSxIVUIwMiwyMDI2LTAyLTIzLDIwMjYtMDItMjcsMjAyNi0wMy0wMSwyNy43LDEzNTAuMiwzMzcuOTMsREVMSVZFUkVELEN1c3RvbWVyIFVuYXZhaWxhYmxlLDIwMjYtMDMtMDEgMTc6MzY6MDAKU0hQMjAyNjAwMTMwMCxDVVNUMDEwMCxDQVIwMDMsSFVCMTQsSFVCMDQsMjAyNi0wMi0yNCwyMDI2LTAyLTI3LDIwMjYtMDItMjcsMTYwLjYsOTk4LjcsMTU5OS44NCxETFZSRCwsMjAyNi0wMi0yNyAwOTowNjowMApTSFAyMDI2MDAxMzA3LENVU1QwMDU4LENBUjAxMCxIVUIwNixIVUIwMiwyMDI2LTAyLTI0LDIwMjYtMDItMjYsMjAyNi0wMi0yNywxNDIuNiwyMDY4LjUsMjAyMjcuMzEsREVMSVZFUkVELFZlaGljbGUgQnJlYWtkb3duLDIwMjYtMDItMjcgMTY6MzY6MDAKU0hQMjAyNjAwMTU0MixDVVNUMDI5NyxDQVIwMDQsSFVCMTUsSFVCMjAsMjAyNi0wMy0wNSwyMDI2LTAzLTA3LDIwMjYtMDMtMDcsOS44LDI4OTIuMSwxNzA2LjE3LERFTElWRVJFRCwsMjAyNi0wMy0wNyAxMToxOTowMApTSFAyMDI2MDAxNjMxLENVU1QwMjk3LENBUjAwMixIVUIwOSxIVUIwOSwyMDI2LTAzLTEwLDIwMjYtMDMtMTIsMjAyNi0wMy0xMiwxNC40LDY3NS42LDE4MC4wLERFTElWRVJFRCwsMjAyNi0wMy0xMiAxNzozNDowMApTSFAyMDI2MDAxNjU0LENVU1QwMjU0LENBUjAwMixIVUIxMixIVUIwMiwyMDI2LTAzLTExLDIwMjYtMDMtMTMsMjAyNi0wMy0xMyw2MS40LDgyNi43LDcwNi4yOCxERUxJVkVSRUQsLDIwMjYtMDMtMTMgMTM6MDE6MDAKU0hQMjAyNjAwMTczMSxDVVNUMDI4NixDQVIwMDEsSFVCMTEsSFVCMTksMjAyNi0wMy0xMywyMDI2LTAzLTE4LDIwMjYtMDMtMTgsOS41LDE3NTIuNywxOTUuNjQsREVMSVZFUkVELCwyMDI2LTAzLTE4IDEzOjM3OjAwClNIUDIwMjYwMDE3NjYsQ1VTVDAxMDAsQ0FSMDEyLEhVQjAxLEhVQjEyLDIwMjYtMDMtMTUsMjAyNi0wMy0xOSwyMDI2LTAzLTE5LDE5NC4zLDgyOC41LDEzNDIuNTMsREVMSVZFUkVELCwyMDI2LTAzLTE5IDA2OjA5OjAwClNIUDIwMjYwMDE4MDMsQ1VTVDAyODAsQ0FSMDAyLEhVQjAyLEhVQjEwLDIwMjYtMDMtMTYsMjAyNi0wMy0xOSwsMzk2LjgsMTQ5My4zLDg4NTguOTUsQ0FOQ0VMTEVELCwyMDI2LTAzLTE2IDE3OjAyOjAwClNIUDIwMjYwMDE5MDksQ1VTVDAzNzcsQ0FSMDAxLEhVQjAxLEhVQjE4LDIwMjYtMDMtMjAsMjAyNi0wMy0yNCwyMDI2LTAzLTIzLDk5LjQsMTA0Ni40LDEyNzYuMjEsREVMSVZFUkVELCwyMDI2LTAzLTIzIDIyOjQ1OjAwClNIUDIwMjYwMDE5MzIsQ1VTVDAxNDgsQ0FSMDA3LEhVQjAzLEhVQjEyLDIwMjYtMDMtMjEsMjAyNi0wMy0yNCwyMDI2LTAzLTI1LDM0My4xLDc0NC42LC0yODYyLjY2LERFTElWRVJFRCxXZWF0aGVyLDIwMjYtMDMtMjUgMDY6MDg6MDAKU0hQMjAyNjAwMjA2OSxDVVNUMDExOSxDQVIwMDUsSFVCMDMsSFVCMDcsMjAyNi0wMy0yNiwyMDI2LTAzLTMxLDIwMjYtMDQtMDMsNy43LDE0NjYuMiwxODAuMCxERUxJVkVSRUQsVHJhZmZpYyBDb25nZXN0aW9uLDIwMjYtMDQtMDMgMTk6Mjk6MDAKU0hQMjAyNjAwMjIwMCxDVVNUMDE1OSxDQVIwMDgsSFVCMTAsSFVCMDcsMjAyNi0wMy0zMSwyMDI2LTA0LTA0LDIwMjYtMDQtMDQsMTAuOSwxNDcwLjksMTgwLjAsREVMSVZFUkVELCwyMDI2LTA0LTA0IDA2OjA5OjAwClNIUDIwMjYwMDIyMzMsQ1VTVDAxNDIsQ0FSMDA3LEhVQjAyLEhVQjA4LDIwMjYtMDQtMDIsMjAyNi0wNC0wNSwsOS4zLDU1MC4wLDE4MC4wLENBTkNFTExFRCwsMjAyNi0wNC0wMiAyMDozNTowMApTSFAyMDI2MDAyNDYwLENVU1QwMTg4LENBUjAwNSxIVUIwNixIVUIwOCwyMDI2LTA0LTEyLDIwMjYtMDQtMTgsMjAyNi0wNC0xOCwyMy45LDIwMjMuNiwzNzQuMjEsREVMSVZFUkVELCwyMDI2LTA0LTE4IDExOjMyOjAwClNIUDIwMjYwMDI1NjEsQ1VTVDAwNzcsQ0FSMDEwLEhVQjE5LEhVQjAzLDIwMjYtMDQtMTYsMjAyNi0wNC0xOCwyMDI2LTA0LTE4LDYuNiwxODI0LjMsODcwLjY4LERFTElWRVJFRCwsMjAyNi0wNC0xOCAxMzowNDowMApTSFAyMDI2MDAyNTg3LENVU1QwMDc2LENBUjAwOSxIVUIxNyxIVUIxOSwyMDI2LTA0LTE3LDIwMjYtMDQtMTksMjAyNi0wNC0xOSwxOTAuNiw4MjguNywyMzE4LjI4LERFTElWRVJFRCwsMjAyNi0wNC0xOSAxODowNDowMApTSFAyMDI2MDAyNjEzLENVU1QwMTAwLENBUjAxMixIVUIwMSxIVUIxNiwyMDI2LTA0LTE4LDIwMjYtMDQtMjIsMjAyNi0wNC0yMiwyMjQuMiw5MDguOCwxODczLjQ4LERFTElWRVJFRCwsMjAyNi0wNC0yMiAwNzoxMTowMApTSFAyMDI2MDAyNjQzLENVU1QwMjk3LENBUjAxMSxIVUIxMSxIVUIwOCwyMDI2LTA0LTIwLDIwMjYtMDQtMjIsLDI0LjMsODc4LjgsMzMyLjY5LENBTkNFTExFRCwsMjAyNi0wNC0yMCAxODoxMjowMApTSFAyMDI2MDAyOTMwLENVU1QwMDUwLENBUjAwNCxIVUIwMixIVUIxNywyMDI2LTA0LTMwLDIwMjYtMDUtMDIsMjAyNi0wNS0wMiwxMTkuMywxODE2LjUsMTM5MDYuNTcsREVMSVZFUkVELCwyMDI2LTA1LTAyIDE4OjQ3OjAwClNIUDIwMjYwMDI5NjUsQ1VTVDAxMDcsQ0FSMDEwLEhVQjAxLEhVQjEyLDIwMjYtMDUtMDEsMjAyNi0wNS0wMiwyMDI2LTA1LTAzLDcuMiw4MjguNSwzOTUuOTUsREVMSVZFUkVELEFkZHJlc3MgSXNzdWUsMjAyNi0wNS0wMyAwNzoxMDowMApTSFAyMDI2MDAyOTk2LENVU1QwMDg0LENBUjAwOCxIVUIwMixIVUIxNiwyMDI2LTA1LTAzLDIwMjYtMDUtMDcsMjAyNi0wNS0wOSwyMC43LDEyNDYuNSwyMzAuNTEsREVMSVZFUkVELFdlYXRoZXIsMjAyNi0wNS0wOSAwNjowNjowMApTSFAyMDI2MDAzMDU1LENVU1QwMjg2LENBUjAwNCxIVUIwMixIVUIwMSwyMDI2LTA1LTA1LDIwMjYtMDUtMDYsMjAyNi0wNS0wNiw0Ni4xLDc3Ni44LDIyNjkuMDcsREVMSVZFUkVELCwyMDI2LTA1LTA2IDEyOjM4OjAwClNIUDIwMjYwMDMwNTcsQ1VTVDAyMDgsQ0FSMDA1LEhVQjI0LEhVQjA2LDIwMjYtMDUtMDUsMjAyNi0wNS0xMSwsMjYuMCwyMjMzLjUsNDU3LjQ4LENBTkNFTExFRCwsMjAyNi0wNS0wNSAyMjo1MTowMApTSFAyMDI2MDAzMTI4LENVU1QwMzkyLENBUjAwNCxIVUIwNyxIVUIwNSwyMDI2LTA1LTA4LDIwMjYtMDUtMDksMjAyNi0wNS0wOSwyNzkuNCwxMTQzLjAsMjA1NzEuODgsREVMSVZFUkVELCwyMDI2LTA1LTA5IDE5OjQyOjAwClNIUDIwMjYwMDMyMTAsQ1VTVDAzODgsQ0FSMDA2LEhVQjAyLEhVQjIxLDIwMjYtMDUtMTIsMjAyNi0wNS0xNSwyMDI2LTA1LTE3LDgyLjEsMjkxLjEsMjA3Ljc2LERFTElWRVJFRCxXZWF0aGVyLDIwMjYtMDUtMTcgMDY6NDE6MDAKU0hQMjAyNjAwMzM0NCxDVVNUMDE0OCxDQVIwMDksSFVCMDYsSFVCMDUsMjAyNi0wNS0xOCwyMDI2LTA1LTIyLDIwMjYtMDUtMjIsMzE0LjUsMTY5OC4wLDc2NDguMDQsREVMSVZFUkVELCwyMDI2LTA1LTIyIDA4OjIyOjAwClNIUDIwMjYwMDMzNTQsQ1VTVDAwNzIsQ0FSMDA3LEhVQjAzLEhVQjA3LDIwMjYtMDUtMTgsMjAyNi0wNS0yMiwyMDI2LTA1LTIyLDEyOC42LDE0NjYuMiwyMjQ5LjIxLERFTElWRVJFRCwsMjAyNi0wNS0yMiAxMjo1MDowMApTSFAyMDI2MDAzMzkwLENVU1QwMjk3LENBUjAwNSxIVUIwNCxIVUIxMywyMDI2LTA1LTE5LDIwMjYtMDUtMjQsMjAyNi0wNS0yNCwxOS45LDEzNzQuMiwxOTUuNzUsREVMSVZFUkVELCwyMDI2LTA1LTI0IDIwOjEwOjAwClNIUDIwMjYwMDM0MjMsQ1VTVDAyOTcsQ0FSMDA1LEhVQjA4LEhVQjExLDIwMjYtMDUtMjEsMjAyNi0wNS0yNSwyMDI2LTA1LTI2LDI3LjQsODc4LjgsMTg4LjY1LERFTElWRVJFRCxEb2N1bWVudGF0aW9uLDIwMjYtMDUtMjYgMTU6MDg6MDAKU0hQMjAyNjAwMzQ0MixDVVNUMDMwMCxDQVIwMDgsSFVCMjEsSFVCMDYsMjEtMDUtMjAyNiwyMDI2LTA1LTI2LDIwMjYtMDUtMjYsMTcuOCwyMDEyLjEsMjkxLjEsREVMSVZFUkVELCwyMDI2LTA1LTI2IDEzOjIyOjAwClNIUDIwMjYwMDM5MjgsQ1VTVDAxMDAsQ0FSMDA5LEhVQjA0LEhVQjAxLDIwMjYtMDYtMTAsMjAyNi0wNi0xMiwyMDI2LTA2LTEyLDUxLjIsNjI1LjAsNDczLjgzLERFTElWRVJFRCwsMjAyNi0wNi0xMiAyMDozNjowMApTSFAyMDI2MDA0MDY4LENVU1QwMDg0LENBUjAwMixIVUIxOCxIVUIwNSwyMDI2LTA2LTE2LDIwMjYtMDYtMTksLDI0LjYsMTI0NS40LDQ4NC4yNixDQU5DRUxMRUQsLDIwMjYtMDYtMTYgMDY6NTA6MDAKU0hQMjAyNjAwNDEzMCxDVVNUMDM0OCxDQVIwMDEsSFVCMDksSFVCMDUsMjAyNi0wNi0xOCwyMDI2LTA2LTI0LDIwMjYtMDYtMjQsNzAuMywyMDA4LjksMTY2My40MyxERUxJVkVSRUQsLDIwMjYtMDYtMjQgMTM6NTI6MDAKU0hQMjAyNjAwNDEzMyxDVVNUMDE0OCxDQVIwMTIsSFVCMDUsSFVCMDgsMjAyNi0wNi0xOCwyMDI2LTA2LTI0LDIwMjYtMDYtMjQsMjI3LjMsMTcxNC44LDMyMzkuMTIsREVMSVZFUkVELCwyMDI2LTA2LTI0IDExOjMxOjAwClNIUDIwMjYwMDQ0NjEsQ1VTVDAwMzAsQ0FSMDEwLEhVQjAzLEhVQjIxLDIwMjYtMDctMDIsMjAyNi0wNy0wMywyMDI2LTA3LTAzLDQuNSwxMTcyLjMsMzU4LjczLERFTElWRVJFRCwsMjAyNi0wNy0wMyAxNzoyMjowMApTSFAyMDI2MDA0NTMyLENVU1QwMTAwLENBUjAwOSxIVUIwNCxIVUIwMSwyMDI2LTA3LTA0LDIwMjYtMDctMDYsMjAyNi0wNy0wNiw3MS4zLDYyNS4wLDYyNi44MyxERUxJVkVSRUQsLDIwMjYtMDctMDYgMTc6MzM6MDAKU0hQMjAyNjAwNDU3OCxDVVNUMDE0OCxDQVIwMDMsSFVCMDQsSFVCMjIsMjAyNi0wNy0wNiwyMDI2LTA3LTA5LDIwMjYtMDctMTEsMTAyMS4zLDY0MC4xLDY3NjcuNzMsREVMSVZFUkVELFdlYXRoZXIsMjAyNi0wNy0xMSAxODowNDowMApTSFAyMDI2MDA0NjAwLENVU1QwMTAwLENBUjAwNCxIVUIwNyxIVUIwNSwyMDI2LTA3LTA3LDIwMjYtMDctMDgsLDEzNC4zLDExNDMuMCwxMDAyOS41NyxDQU5DRUxMRUQsLDIwMjYtMDctMDcgMTM6MzU6MDAKU0hQMjAyNjAwNDY5MixDVVNUMDA3NyxDQVIwMTIsSFVCMTIsSFVCMTAsMjAyNi0wNy0xMCwyMDI2LTA3LTE0LDIwMjYtMDctMTQsNy45LDY2OC4xLDE4MC4wLERFTElWRVJFRCwsMjAyNi0wNy0xNCAxNzoyOTowMApTSFAyMDI2MDA1MDk2LENVU1QwMDUyLENBUjAxMCxIVUIwMixIVUIxNiwyMDI2LTA3LTI3LDIwMjYtMDctMjgsMjAyNi0wNy0yOCwxMC4xLDEyNDYuNSw4MzIuNzIsUkVUVVJORUQsLDIwMjYtMDctMjggMTQ6MTQ6MDAKU0hQMjAyNjAwNTEyMixDVVNUMDEwMCxDQVIwMDcsSFVCMDcsSFVCMDUsMjAyNi0wNy0yOCwyMDI2LTA4LTAxLDIwMjYtMDctMzEsNDYuNCwxMTQzLjAsNjMyLjYyLERFTElWRVJFRCwsMjAyNi0wNy0zMSAwOTozMTowMApTSFAyMDI2MDA1Mjk2LENVU1QwMTAwLENBUjAwOCxIVUIwMyxIVUIxNCwyMDI2LTA4LTA0LDIwMjYtMDgtMDgsMjAyNi0wOC0wOCw1OC45LDE3MDEuMSw5MTUuNTQsREVMSVZFUkVELCwyMDI2LTA4LTA4IDIwOjM2OjAwClNIUDIwMjYwMDU1MTIsQ1VTVDAxMDAsQ0FSMDA4LEhVQjA1LEhVQjAyLDIwMjYtMDgtMTIsMjAyNi0wOC0xNiwyMDI2LTA4LTE2LDM0LjksMTI5MS40LDM3OC45NCxERUxJVkVSRUQsLDIwMjYtMDgtMTYgMTU6MDc6MDAKU0hQMjAyNjAwNTYyNSxDVVNUMDA3MixDQVIwMDEsSFVCMDksSFVCMDEsMjAyNi0wOC0xOCwyMDI2LTA4LTIyLDIwMjYtMDgtMjIsOTUuOCwxMzY4LjksMTU1My4wOSxERUxJVkVSRUQsLDIwMjYtMDgtMjIgMTI6NDQ6MDAKU0hQMjAyNjAwNTczNSxDVVNUMDEwMCxDQVIwMTEsSFVCMTAsSFVCMDEsMjAyNi0wOC0yMiwyMDI2LTA4LTI1LDIwMjYtMDgtMjUsNjguOCwxMzUyLjYsMTQ2NC41NixSRVRVUk5FRCwsMjAyNi0wOC0yNSAyMTo1MDowMApTSFAyMDI2MDA1NzY5LENVU1QwMDc5LENBUjAxMCxIVUIwOCxIVUIwNCwyMDI2LTA4LTI0LDIwMjYtMDgtMjUsMjAyNi0wOC0yOSw3My44LDE1NDYuMyw3Njc1LjU4LFJFVFVSTkVELFdlYXRoZXIsMjAyNi0wOC0yOSAwNjo0NDowMApTSFAyMDI2MDA1ODk1LENVU1QwMjM3LENBUjAwMyxIVUIwNSxIVUIwMSwyMDI2LTA4LTI4LDIwMjYtMDgtMzEsMjAyNi0wOS0wMSw5LjAsNjQ0LjEsMTgwLjAsREVMSVZFUkVELFRyYWZmaWMgQ29uZ2VzdGlvbiwyMDI2LTA5LTAxIDEyOjUyOjAwClNIUDIwMjYwMDU5MDMsQ1VTVDAxMDAsQ0FSMDA3LEhVQjA1LEhVQjIxLDIwMjYtMDgtMjgsMjAyNi0wOS0wMSwyMDI2LTA5LTAxLDQ1LjUsMTQ5Ni4wLDg1NS44LFJFVFVSTkVELCwyMDI2LTA5LTAxIDE5OjQzOjAwClNIUDIwMjYwMDU5MzEsQ1VTVDAzMTcsQ0FSMDEyLEhVQjEzLEhVQjAxLDIwMjYtMDgtMjksMjAyNi0wOS0wMiwyMDI2LTA5LTAyLDExLjIsODE3LjAsMTgwLjAsREVMSVZFUkVELCwyMDI2LTA5LTAyIDE2OjE5OjAwClNIUDIwMjYwMDYwMzYsQ1VTVDAxMDAsQ0FSMDEyLEhVQjA3LEhVQjAxLDIwMjYtMDktMDMsMjAyNi0wOS0wNywyMDI2LTA5LTA4LDI4NS43LDYzMi4yLDE2NTkuNjIsREVMSVZFUkVELFdlYXRoZXIsMjAyNi0wOS0wOCAxNjo0OTowMApTSFAyMDI2MDA2Mjk4LENVU1QwMzM3LENBUjAwMixIVUIwNyxIVUIwNSwyMDI2LTA5LTE0LDIwMjYtMDktMTcsMjAyNi0wOS0xNiwyMC45LDExNDMuMCwzNDQuMDMsREVMSVZFUkVELCwyMDI2LTA5LTE2IDE0OjU2OjAwClNIUDIwMjYwMDYzMDUsQ1VTVDAzNzgsQ0FSMDAxLEhVQjAyLEhVQjA4LDIwMjYtMDktMTQsMjAyNi0wOS0xNywyMDI2LTA5LTE4LDMxMS41LDU1MC4wLDE5MDkuMzksREVMSVZFUkVELFdlYXRoZXIsMjAyNi0wOS0xOCAxNzoxOTowMApTSFAyMDI2MDA2MzUzLENVU1QwMTM0LENBUjAwNSxIVUIwMyxIVUIwNywyMDI2LTA5LTE2LDIwMjYtMDktMjEsLDExOC43LDE0NjYuMiwxMjk4LjMyLENBTkNFTExFRCwsMjAyNi0wOS0xNiAxNjozODowMApTSFAyMDI2MDA2NDgxLENVU1QwMjUzLENBUjAwNSxIVUIwNyxIVUIwOCwyMDI2LTA5LTIxLDIwMjYtMDktMjUsMjAyNi0wOS0yNSw4LjYsNjQ3LjcsMTgwLjAsREVMSVZFUkVELCwyMDI2LTA5LTI1IDA3OjM5OjAwClNIUDIwMjYwMDY1MjEsQ1VTVDAxODgsQ0FSMDExLEhVQjA2LEhVQjAyLDIwMjYtMDktMjIsMjAyNi0wOS0yNywyMDI2LTA5LTI3LDEwLjAsMjA2OC41LDMxNy43NixERUxJVkVSRUQsLDIwMjYtMDktMjcgMTA6MjU6MDAKU0hQMjAyNjAwNjUyNyxDVVNUMDE4OCxDQVIwMTIsSFVCMDEsSFVCMDMsMjAyNi0wOS0yMywyMDI2LTA5LTI5LDIwMjYtMTAtMDEsOC4wLDE1NjkuMiwxODAuMCxERUxJVkVSRUQsV2VhdGhlciwyMDI2LTEwLTAxIDA4OjQ5OjAwClNIUDIwMjYwMDY1OTUsQ1VTVDAxOTUsQ0FSMDAxLEhVQjAxLEhVQjAzLDIwMjYtMDktMjYsMjAyNi0xMC0wMSwyMDI2LTEwLTAxLDguNiwxNTY5LjIsMTgwLjAsREVMSVZFUkVELCwyMDI2LTEwLTAxIDA4OjAxOjAwClNIUDIwMjYwMDY3MzgsQ1VTVDAwMzIsQ0FSMDExLEhVQjA3LEhVQjA5LDIwMjYtMTAtMDEsMjAyNi0xMC0wNCwyMDI2LTEwLTA0LDYuOSwxMTkyLjMsMTkwLjgsREVMSVZFUkVELCwyMDI2LTEwLTA0IDE4OjUyOjAwClNIUDIwMjYwMDY4OTQsQ1VTVDAxOTksQ0FSMDA4LEhVQjAzLEhVQjAyLDIwMjYtMTAtMDUsMjAyNi0xMC0wOSwyMDI2LTEwLTExLDIwMy4wLDE0MzUuMSwyNjc2LjA0LERFTElWRVJFRCxUcmFmZmljIENvbmdlc3Rpb24sMjAyNi0xMC0xMSAxMDo1NTowMApTSFAyMDI2MDA3MDI3LENVU1QwMzU4LENBUjAwNyxIVUIwNyxIVUIwMiwyMDI2LTEwLTA5LDIwMjYtMTAtMTEsMjAyNi0xMC0xMCwzNjAuNywxNTAuMiw3MTMuOTEsREVMSVZFUkVELCwyMDI2LTEwLTEwIDIwOjU0OjAwClNIUDIwMjYwMDcwNjAsQ1VTVDAyODYsQ0FSMDAxLEhVQjA0LEhVQjE2LDIwMjYtMTAtMTAsMjAyNi0xMC0xMiwyMDI2LTEwLTEyLDMxLjUsMjg1LjIsMTkwLjgsUkVUVVJORUQsLDIwMjYtMTAtMTIgMDg6NDQ6MDAKU0hQMjAyNjAwNzEwNixDVVNUMDM0OCxDQVIwMDMsSFVCMTMsSFVCMTgsMjAyNi0xMC0xMiwyMDI2LTEwLTE2LDIwMjYtMTAtMTYsMjguOSwxMzMxLjgsNDIyLjE5LERFTElWRVJFRCwsMjAyNi0xMC0xNiAxMzowNTowMApTSFAyMDI2MDA3NTYyLENVU1QwMzgyLENBUjAwNyxIVUIxMCxIVUIwNSwyMDI2LTEwLTIzLDIwMjYtMTAtMjgsMjAyNi0xMC0yOCw0LjAsMTkxNS4xLDE5MC44LERFTElWRVJFRCwsMjAyNi0xMC0yOCAxNTo0MTowMApTSFAyMDI2MDA3Njk2LENVU1QwMDc3LENBUjAwNixIVUIwNCxIVUIyMSwyMDI2LTEwLTI4LDIwMjYtMTEtMDIsMjAyNi0xMS0wMiwxMS41LDEzMDMuMiwxOTAuOCxSRVRVUk5FRCwsMjAyNi0xMS0wMiAyMjo0MjowMApTSFAyMDI2MDA3NzQ5LENVU1QwMjk3LENBUjAxMCxIVUIxMCxIVUIwMiwyMDI2LTEwLTI5LDIwMjYtMTAtMzAsMjAyNi0xMC0zMCwyMC43LDE0OTMuMywyMzc5Ljg2LERFTElWRVJFRCwsMjAyNi0xMC0zMCAwNzozNzowMApTSFAyMDI2MDA4MDMzLENVU1QwMzQxLENBUjAxMSxIVUIxMCxIVUIxNywyMDI2LTExLTA2LDIwMjYtMTEtMDgsMjAyNi0xMS0wOCw0MS40LDU1MC44LDM2My4xNyxERUxJVkVSRUQsLDIwMjYtMTEtMDggMTQ6MTU6MDAKU0hQMjAyNjAwODE5NixDVVNUMDIwNyxDQVIwMDgsSFVCMTcsSFVCMTYsMjAyNi0xMS0xMSwyMDI2LTExLTE2LCw5LjUsMjI5NC4wLDE5MC44LElOX1RSQU5TSVQsLDIwMjYtMTEtMTEgMDg6MDA6MDAKU0hQMjAyNjAwODE5OCxDVVNUMDI5NyxDQVIwMDcsSFVCMDUsSFVCMDcsMjAyNi0xMS0xMSwyMDI2LTExLTE1LCwxOS4wLDExNDMuMCwyNjkuOTcsQ0FOQ0VMTEVELCwyMDI2LTExLTExIDExOjU4OjAwClNIUDIwMjYwMDgzNjksQ1VTVDAzNjYsQ0FSMDA4LEhVQjIyLEhVQjE5LDIwMjYtMTEtMTUsMjAyNi0xMS0yMCwsMTQ2LjAsMTk2MS42LDI1MTguNTksSU5fVFJBTlNJVCwsMjAyNi0xMS0xNSAxNzowMDowMApTSFAyMDI2MDA4NDIyLENVU1QwMDMyLENBUjAwMyxIVUIwMixIVUIyMCwyMDI2LTExLTE2LDIwMjYtMTEtMjEsLDguNSwxNjkyLjksMTkwLjgsSU5fVFJBTlNJVCwsMjAyNi0xMS0xNiAxMDowMDowMApTSFAyMDI2MDA4NDI2LENVU1QwMjAxLENBUjAwOSxIVUIwNixIVUIwNywyMDI2LTExLTE2LDIwMjYtMTEtMjAsLDIwMC40LDE5NjkuMiw2MDgwLjczLElOX1RSQU5TSVQsLDIwMjYtMTEtMTYgMTc6MDA6MDAKU0hQMjAyNjAwODQ2OSxDVVNUMDM0MyxDQVIwMDQsSFVCMDEsSFVCMDMsMjAyNi0xMS0xNywyMDI2LTExLTE4LCw5MS4yLDE1NjkuMiw5MDUyLjMsSU5fVFJBTlNJVCwsMjAyNi0xMS0xNyAxNzowMDowMApTSFAyMDI2MDA4NDc3LENVU1QwMTYwLENBUjAwMixIVUIwOCxIVUIwNSwyMDI2LTExLTE3LDIwMjYtMTEtMjEsLDE0LjIsMTcxNC44LDM5MC42LElOX1RSQU5TSVQsLDIwMjYtMTEtMTcgMTE6MDA6MDAKU0hQMjAyNjAwODQ4MixDVVNUMDA4NCxDQVIwMDQsSFVCMDksSFVCMDIsMjAyNi0xMS0xNywyMDI2LTExLTE4LCw0My41LDExNTAuOSwzMjkxLjEsSU5fVFJBTlNJVCwsMjAyNi0xMS0xNyAwOTowMDowMApTSFAyMDI2MDA4NDk2LENVU1QwMzQxLENBUjAwMyxIVUIwMyxIVUIwNSwyMDI2LTExLTE3LDIwMjYtMTEtMjMsLDM2LjYsMjE5NC43LDg2NS4yOCxJTl9UUkFOU0lULCwyMDI2LTExLTE3IDE1OjAwOjAwClNIUDIwMjYwMDg0OTcsQ1VTVDAxMDAsQ0FSMDA2LEhVQjA1LEhVQjAxLDIwMjYtMTEtMTcsMjAyNi0xMS0yMSwsNjYuMiw2NDQuMSw0MzQuNCxJTl9UUkFOU0lULCwyMDI2LTExLTE3IDEwOjAwOjAwClNIUDIwMjYwMDg1MDEsQ1VTVDAxNjksQ0FSMDEwLEhVQjAzLEhVQjA0LDIwMjYtMTEtMTcsMjAyNi0xMS0xOSwsMTg3LjQsMjE3NC44LDI3ODgzLjI3LElOX1RSQU5TSVQsLDIwMjYtMTEtMTcgMjA6MDA6MDAKU0hQMjAyNjAwODUwNCxDVVNUMDA3NyxDQVIwMDUsSFVCMDQsSFVCMTcsMjAyNi0xMS0xNywyMDI2LTExLTIzLCwzMi4xLDIwMTIuOSw0OTEuMyxJTl9UUkFOU0lULCwyMDI2LTExLTE3IDA4OjAwOjAwClNIUDIwMjYwMDg1MDYsQ1VTVDAxODksQ0FSMDEwLEhVQjA0LEhVQjAzLDIwMjYtMTEtMTcsMjAyNi0xMS0xOSwsNjk1LjYsMjE3NC44LDEwMzc1MC4zLElOX1RSQU5TSVQsLDIwMjYtMTEtMTcgMTA6MDA6MDAKU0hQMjAyNjAwODUxNixDVVNUMDI5NyxDQVIwMDEsSFVCMDQsSFVCMDMsMjAyNi0xMS0xNywyMDI2LTExLTIzLCw0Ni4wLDIxNzQuOCwxMTI4LjcyLElOX1RSQU5TSVQsLDIwMjYtMTEtMTcgMTE6MDA6MDAKU0hQMjAyNjAwODUyMCxDVVNUMDEzNSxDQVIwMDgsSFVCMTEsSFVCMDIsMjAyNi0xMS0xNywyMDI2LTExLTIwLCwyMTUuMyw4NjAuMCwxNzE1LjMxLElOX1RSQU5TSVQsLDIwMjYtMTEtMTcgMTk6MDA6MDAKU0hQMjAyNjAwODUzNixDVVNUMDI4NSxDQVIwMDgsSFVCMDQsSFVCMDcsMjAyNi0xMS0xNywyMDI2LTExLTIwLCwxMDYuNSw5MTkuMCw4MjUuODMsSU5fVFJBTlNJVCwsMjAyNi0xMS0xNyAxODowMDowMApTSFAyMDI2MDA4NTY1LENVU1QwMjU0LENBUjAxMSxIVUIwNyxIVUIxMSwyMDI2LTExLTE4LDIwMjYtMTEtMjAsLDI2Mi42LDc3NS4yLDM2ODcuNjMsSU5fVFJBTlNJVCwsMjAyNi0xMS0xOCAxMTowMDowMApTSFAyMDI2MDA4NTc2LENVU1QwMDA5LENBUjAwMyxIVUIxNSxIVUIwMywyMDI2LTExLTE4LDIwMjYtMTEtMjUsLDY3OS4yLDI1OTkuNywyMDk5MS43MyxJTl9UUkFOU0lULCwyMDI2LTExLTE4IDE0OjAwOjAwClNIUDIwMjYwMDg1OTgsQ1VTVDAwNzcsQ0FSMDAxLEhVQjIyLEhVQjAxLDIwMjYtMTEtMTgsMjAyNi0xMS0yMCwsNTYuNCwzMTIuMiwyMDcuODUsSU5fVFJBTlNJVCwsMjAyNi0xMS0xOCAxNzowMDowMApTSFAyMDI2MDA4NjAyLENVU1QwMTQ0LENBUjAwNCxIVUIyMixIVUIyNCwyMDI2LTExLTE4LDIwMjYtMTEtMTksLDIzLjEsOTI0LjgsMTM2NC41NixJTl9UUkFOU0lULCwyMDI2LTExLTE4IDE0OjAwOjAwClNIUDIwMjYwMDg2MTAsQ1VTVDAwNDIsQ0FSMDAzLEhVQjAyLEhVQjIxLDIwMjYtMTEtMTgsMjAyNi0xMS0yMCwsMTkwLjcsMjkxLjEsNjM3LjcsSU5fVFJBTlNJVCwsMjAyNi0xMS0xOCAxMzowMDowMApTSFAyMDI2MDA4NjExLENVU1QwMzIyLENBUjAwOSxIVUIyMixIVUIwMSwyMDI2LTExLTE4LDIwMjYtMTEtMTksLDE3LjIsMzEyLjIsMTkwLjgsQk9PS0VELCwyMDI2LTExLTE4IDEyOjAwOjAwClNIUDIwMjYwMDg2MTYsQ1VTVDAzNTUsQ0FSMDAyLEhVQjA2LEhVQjIzLDIwMjYtMTEtMTgsMjAyNi0xMS0yMSwsMzMuNiwxMzA5LjgsNjY2LjgsSU5fVFJBTlNJVCwsMjAyNi0xMS0xOCAxMDowMDowMApTSFAyMDI2MDA4NjQ0LENVU1QwMDE4LENBUjAwNixIVUIxNyxIVUIwMSwyMDI2LTExLTE5LDIwMjYtMTEtMjQsLDIwMzMuOSwxNDI4LjIsMjg4MDUuMTQsQk9PS0VELCwyMDI2LTExLTE5IDA1OjQyOjAwClNIUDIwMjYwMDg2NDgsQ1VTVDAwOTMsQ0FSMDA5LEhVQjA3LEhVQjEwLDIwMjYtMTEtMTksMjAyNi0xMS0yMiwsMTMuOCwxNDcwLjksMjkzLjQzLElOX1RSQU5TSVQsLDIwMjYtMTEtMTkgMDU6NTk6MDAKU0hQMjAyNjAwODY1MixDVVNUMDAzOSxDQVIwMDcsSFVCMDQsSFVCMDIsMjAyNi0xMS0xOSwyMDI2LTExLTIzLCwyNTAuNCwxMDU2LjYsMzUxNS4yNCxCT09LRUQsLDIwMjYtMTEtMTkgMDU6MzE6MDAKU0hQMjAyNjAwODY2OSxDVVNUMDM5MSxDQVIwMDksSFVCMDUsSFVCMjAsMjAyNi0xMS0xOSwyMDI2LTExLTI0LCw0NjEuNywyNDkzLjgsMTY4ODkuOTksSU5fVFJBTlNJVCwsMjAyNi0xMS0xOSAwNToyNDowMApTSFAyMDI2MDA4NjczLENVU1QwMjk3LENBUjAwOCxIVUIwMyxIVUIwMiwyMDI2LTExLTE5LDIwMjYtMTEtMjMsLDI0LjYsMTQzNS4xLDM0MS4zNyxCT09LRUQsLDIwMjYtMTEtMTkgMDU6NDg6MDAK"
  )
  inputs.foreach { case (entity, encoded) =>
    val content = new String(java.util.Base64.getDecoder.decode(encoded), java.nio.charset.StandardCharsets.UTF_8)
    dbutils.fs.mkdirs(s"$sourceRoot/$entity")
    require(dbutils.fs.put(s"$sourceRoot/$entity/$entity.csv", content, overwrite = true), s"Could not stage $entity.csv")
  }
}

BatchJob.run(spark, sourceRoot, s"$lakeRoot/batch", Some(namespace))

// COMMAND ----------
val audit = spark.read.format("delta").load(s"$lakeRoot/batch/audit/batch_counts")
display(audit)
Seq("monthly_delivery", "carrier_scorecard", "lane_performance", "delay_mix", "segment_revenue").foreach { name =>
  val delta = spark.read.format("delta").load(s"$lakeRoot/batch/gold/$name")
  val iceberg = spark.table(s"$namespace.$name").select(delta.columns.map(col): _*)
  require(delta.exceptAll(iceberg).isEmpty && iceberg.exceptAll(delta).isEmpty, s"Delta and Iceberg differ for $name")
}

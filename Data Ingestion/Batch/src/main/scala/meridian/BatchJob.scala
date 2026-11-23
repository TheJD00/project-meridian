package meridian

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

param([string]$OutputPath = (Join-Path $PSScriptRoot 'MeridianBatch.scala'))

$ErrorActionPreference = 'Stop'
$source = Join-Path $PSScriptRoot '../src/main/scala/meridian/BatchJob.scala'
$job = (Get-Content -LiteralPath $source -Raw) -replace '^package meridian\s+', ''
$inputs = foreach ($entity in 'hubs','carriers','customers','shipments') {
    $file = Join-Path $PSScriptRoot "../../../Datasets/batch/raw/$entity/$entity.csv"
    $encoded = [Convert]::ToBase64String([IO.File]::ReadAllBytes($file))
    '  "' + $entity + '" -> "' + $encoded + '"'
}
$runner = @'
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
__INPUTS__
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
'@
$runner = $runner.Replace('__INPUTS__', ($inputs -join ",`n"))
Set-Content -LiteralPath $OutputPath -Value ("// Databricks notebook source" + [Environment]::NewLine + $job + [Environment]::NewLine + $runner) -Encoding ascii

// Databricks notebook source
dbutils.widgets.text("raw_root", "")
dbutils.widgets.text("delta_root", "abfss://adlsgen2jdcontainer@adlsgen2jd.dfs.core.windows.net/meridian/batch")
dbutils.widgets.text("iceberg_namespace", "")

val rawRoot = dbutils.widgets.get("raw_root").trim.stripSuffix("/")
val deltaRoot = dbutils.widgets.get("delta_root").trim.stripSuffix("/")
val icebergNamespace = Option(dbutils.widgets.get("iceberg_namespace").trim).filter(_.nonEmpty)
require(rawRoot.nonEmpty && deltaRoot.nonEmpty, "Set raw_root and delta_root before running")
meridian.BatchJob.run(spark, rawRoot, deltaRoot, icebergNamespace)

-- Databricks notebook source
-- Replace main if the Meridian schema was deployed to a different catalog.
USE CATALOG main;
USE SCHEMA meridian;

-- COMMAND ----------
DESCRIBE DETAIL monthly_delivery;

-- COMMAND ----------
DESCRIBE HISTORY monthly_delivery;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC For time travel, query `monthly_delivery VERSION AS OF <version>` using the earlier version shown above.

-- COMMAND ----------
-- Run the schema evolution demonstration on a separate table to preserve the KPI contract.
CREATE TABLE IF NOT EXISTS iceberg_schema_demo
USING iceberg
AS SELECT * FROM monthly_delivery;

-- COMMAND ----------
ALTER TABLE iceberg_schema_demo ADD COLUMNS (review_note STRING);

-- COMMAND ----------
SELECT ship_month, bookings, review_note FROM iceberg_schema_demo ORDER BY ship_month;

-- COMMAND ----------
DESCRIBE HISTORY iceberg_schema_demo;

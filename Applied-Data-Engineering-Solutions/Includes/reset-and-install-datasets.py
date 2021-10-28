# Databricks notebook source
# MAGIC %run ./ade-setup $mode="reset"

# COMMAND ----------

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS source_{database} CASCADE")
    dbutils.fs.rm(Paths.source, True)
    dbutils.fs.rm(Paths.raw, True)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS source_{database}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS source_{database}.producer
  DEEP CLONE delta.`{URI}/bronze`
  LOCATION '{Paths.producer}'
""")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS bronze_dev
  SHALLOW CLONE source_{database}.producer
  LOCATION '{Paths.bronzeDevTable}'
""")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS source_{database}.producer_30m
  DEEP CLONE delta.`{URI}/kafka-30min`
  LOCATION '{Paths.producer30m}'
""")


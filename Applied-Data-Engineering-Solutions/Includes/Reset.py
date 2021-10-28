# Databricks notebook source
# MAGIC %run ./_user $lesson="reset"

# COMMAND ----------

databases = spark.sql("show databases").collect()
for row in databases:
  database = row[0]
  if database.startswith(f"dbacademy_{course}_"):
    print(f"Dropping {database}")
    spark.sql(f"DROP DATABASE {database} CASCADE")

dbutils.fs.rm(userhome_prefix, True)


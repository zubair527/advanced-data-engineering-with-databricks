# Databricks notebook source
# MAGIC %md
# MAGIC # Data Producer
# MAGIC This notebook is designed to automate the process of raw data files landing in object storage so that they can be picked up by our Structured Streaming pipelines.
# MAGIC 
# MAGIC This notebook is designed for Run All execution, but each execution will fully reset the project directory, as well as the state of the data source. 
# MAGIC 
# MAGIC Note that the final cell will continue to execute for approximately 30 minutes (this means that streaming data is actively arriving).
# MAGIC 
# MAGIC This notebook should be executed just before beginning the bronze ingestion notebook.

# COMMAND ----------

import pyspark.sql.functions as F
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %run ./ade-setup

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS source_{database}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS source_{database}.producer_30m
  DEEP CLONE delta.`{URI}/kafka-30min`
  LOCATION '{Paths.producer30m}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify the interval for "arriving" data
# MAGIC The `arrival` field was artificially appended to the dataset in order to allow for filtering below.

# COMMAND ----------

producerDF = spark.table(f"source_{database}.producer_30m")
arrival_max, arrival_min = producerDF.select(F.max("arrival"), F.min("arrival")).collect()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset `sourceDir`
# MAGIC 
# MAGIC The `sourceDir` is the source directory for our streaming data ingestion. We'll land JSON files there to be picked up by our Structured Streaming workloads.

# COMMAND ----------

reset = True
if reset == "True":
  dbutils.fs.rm(Paths.source30m, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land 30 minutes of 5 second batches in approximate real time
# MAGIC 
# MAGIC The code below is imperfect in its timing, but will write new JSON data files to the `sourceDir` every 5-10 seconds until it has gone through the approximately 360 batches of data representing our 30 minute window.

# COMMAND ----------

for batch in range(arrival_min, arrival_max +1):
    start = datetime.now()
    (producerDF.filter(F.col("arrival") == batch).drop("arrival")
        .write
        .mode("append")
        .format("json")
        .save(Paths.source30m))
    print((datetime.now()-start))
    time.sleep(5)



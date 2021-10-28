# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Deduplication
# MAGIC 
# MAGIC In this notebook, you'll learn how to eliminate duplicate records while working with Structured Streaming and Delta Lake.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Apply `dropDuplicates` to streaming data
# MAGIC - Use watermarking to manage state information
# MAGIC - Write an insert-only merge to prevent inserting duplicate records into a Delta table
# MAGIC - Use `foreachBatch` to perform a streaming upsert

# COMMAND ----------

# MAGIC %md
# MAGIC Declare database and set all path variables.

# COMMAND ----------

# MAGIC %run ../Includes/silver-setup

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell deletes the target silver table and checkpoint for idempotent demo execution.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS heart_rate_silver")
dbutils.fs.rm(Paths.silverRecordingsTable, True)
dbutils.fs.rm(Paths.silverRecordingsCheckpoint, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Duplicate Records
# MAGIC 
# MAGIC Because Kafka provides at-least-once guarantees on data delivery, all Kafka consumers should be prepared to handle duplicate records. The de-duplication methods shown here can also be applied when necessary in other parts of your Delta Lake applications.
# MAGIC 
# MAGIC Let's start by identifying the number of duplicate records in our `bpm` topic of the bronze table.

# COMMAND ----------

(spark.read
  .table("bronze")
  .filter("topic = 'bpm'")
  .count()
)

# COMMAND ----------

(spark.read
  .table("bronze")
  .filter("topic = 'bpm'")
  .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
  .select("v.*")
  .dropDuplicates(["device_id", "time"])
  .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC It appears that around 10-20% of our records are duplicates. Note that here we're choosing to apply deduplication at the silver rather than the bronze level. While we are storing some duplicate records, our bronze table retains a history of the true state of our streaming source, presenting all records as they arrived (with some additional metadata recorded). This allows us to recreate any state of our downstream system, if necessary, and prevents potential data loss due to overly aggressive quality enforcement at the initial ingestion as well as minimizing latencies for data ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Streaming Read on the Bronze BPM Records
# MAGIC 
# MAGIC Here we'll bring back in our final logic from our last notebook.

# COMMAND ----------

bpmDF = (spark.readStream
  .table("bronze")
  .filter("topic = 'bpm'")
  .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
  .select("v.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC When dealing with streaming deduplication, there is a level of complexity compared to static data.
# MAGIC 
# MAGIC As each micro-batch is processed, we need to ensure:
# MAGIC - No duplicate records exist in the microbatch
# MAGIC - Records to be inserted are not already in the target table
# MAGIC 
# MAGIC Spark Structured Streaming can track state information for the unique keys to ensure that duplicate records do not exist within or between microbatches. Over time, this state information will scale to represent all history. Applying a watermark of appropriate duration allows us to only track state information for a window of time in which we reasonably expect records could be delayed. Here, we'll define that watermark as 30 seconds.
# MAGIC 
# MAGIC The cell below updates our previous query.

# COMMAND ----------

dedupedDF = bpmDF = (spark.readStream
  .table("bronze")
  .filter("topic = 'bpm'")
  .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
  .select("v.*")
  .withWatermark("time", "30 seconds")
  .dropDuplicates(["device_id", "time"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Only Merge
# MAGIC Delta Lake has optimized functionality for insert-only merges. This operation is ideal for de-duplication: define logic to match on unique keys, and only insert those records for keys that don't already exist.
# MAGIC 
# MAGIC Note that in this application, we proceed in this fashion because we know two records with the same matching keys represent the same information. If the later arriving records indicated a necessary change to an existing record, we would need to change our logic to include a `WHEN MATCHED` clause.
# MAGIC 
# MAGIC A merge into query is defined in SQL below against a view titled `stream_updates`.

# COMMAND ----------

query = """
  MERGE INTO heart_rate_silver a
  USING stream_updates b
  ON a.device_id=b.device_id AND a.time=b.time
  WHEN NOT MATCHED THEN INSERT *
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Microbatch Function for `foreachBatch`
# MAGIC 
# MAGIC The Spark Structured Streaming `foreachBatch` method allows users to define custom logic when writing.
# MAGIC 
# MAGIC The logic applied during `foreachBatch` addresses the present microbatch as if it were a batch (rather than streaming) data.
# MAGIC 
# MAGIC The class defined in the following cell defines simple logic that will allow us to register any SQL `MERGE INTO` query for use in a Structured Streaming write.

# COMMAND ----------

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsertToDelta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.query)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we're using SQL to write to our Delta table, we'll need to make sure this table exists before we begin.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS heart_rate_silver")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS heart_rate_silver
  (device_id LONG, time TIMESTAMP, heartrate DOUBLE)
  USING DELTA
  LOCATION '{Paths.silverRecordingsTable}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Now pass the previously define SQL query to the `Upsert` class.

# COMMAND ----------

streamingMerge=Upsert(query)

# COMMAND ----------

# MAGIC %md
# MAGIC And then use this class in our `foreachBatch` logic.

# COMMAND ----------

(dedupedDF.writeStream
   .foreachBatch(streamingMerge.upsertToDelta)
   .outputMode("update")
   .option("checkpointLocation", Paths.silverRecordingsCheckpoint)
   .trigger(once=True)
   .start()
   .awaitTermination(300))

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that our number of unique entries that have been processed to the `heart_rate_silver` table matches our batch de-duplication query from above.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM heart_rate_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

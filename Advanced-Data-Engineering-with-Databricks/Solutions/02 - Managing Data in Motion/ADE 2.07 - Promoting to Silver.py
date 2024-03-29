# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoting to Silver
# MAGIC 
# MAGIC Here we'll pull together the concepts of streaming from Delta Tables, deduplication, and quality enforcement to finalize our approach to our silver table.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_heartrate_silver.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Apply table constraints to Delta Lake tables
# MAGIC - Use flagging to identify records failing to meet certain conditions
# MAGIC - Apply de-duplication within an incremental microbatch
# MAGIC - Use `MERGE` to avoid inserting duplicate records to a Delta Lake table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../Includes/silver-setup

# COMMAND ----------

# MAGIC %md
# MAGIC Begin by resetting your table and checkpoint to make sure there are no conflicts from previous writes.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS heart_rate_silver")

dbutils.fs.rm(Paths.silverRecordingsTable, True)
dbutils.fs.rm(Paths.silverRecordingsCheckpoint, True)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS heart_rate_silver
(device_id LONG, time TIMESTAMP, heartrate DOUBLE, bpm_check STRING)
USING DELTA
LOCATION '{Paths.silverRecordingsTable}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Constraint
# MAGIC Add a table constraint before inserting data. Name this constraint `dateWithinRange` and make sure that the time is greater than January 1, 2017.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC ALTER TABLE heart_rate_silver ADD CONSTRAINT dateWithinRange CHECK (time > '2017-01-01');

# COMMAND ----------

# MAGIC %md
# MAGIC Note that adding and removing constraints is recorded in the transaction log.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Streaming Read and Transformation
# MAGIC Use the cell below to create a streaming read that includes:
# MAGIC 1. A filter for the topic `bpm`
# MAGIC 2. Logic to flatten the JSON payload and cast data to the appropriate schema
# MAGIC 3. A `bpm_check` column to flag negative records
# MAGIC 4. A duplicate check on `device_id` and `time` with a 30 second watermark on `time`

# COMMAND ----------

# ANSWER

streamingDF = (spark.readStream
  .table("bronze")
  .filter("topic = 'bpm'")
  .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
  .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check"))
  .withWatermark("time", "30 seconds")
  .dropDuplicates(["device_id", "time"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Upsert Query
# MAGIC Below, the upsert class used in the previous notebooks is provided.

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
# MAGIC Use the cell below to define the upsert query to instantiate our class. Alternately, [consult the documentation](https://docs.databricks.com/delta/delta-update.html#upsert-into-a-table-using-merge&language-python) and try implementing this using the `DeltaTable` Python class.

# COMMAND ----------

# ANSWER
query = """
  MERGE INTO heart_rate_silver a
  USING stream_updates b
  ON a.device_id=b.device_id AND a.time=b.time
  WHEN NOT MATCHED THEN INSERT *
"""

streamingMerge=Upsert(query)

# COMMAND ----------

# ANSWER
from delta.tables import *

deltaTable = DeltaTable.forName(spark, "heart_rate_silver")

def upsertToDelta(microBatchDF, batch):
    (deltaTable.alias("a").merge(
        microBatchDF.alias("b"),
        "a.device_id=b.device_id")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Upsert and Write
# MAGIC Now execute a write with trigger once logic to process all existing data from the bronze table.

# COMMAND ----------

def process_silver_heartrate():
    (streamingDF.writeStream
       .foreachBatch(streamingMerge.upsertToDelta)
       .outputMode("update")
       .option("checkpointLocation", Paths.silverRecordingsCheckpoint)
       .trigger(once=True)
       .start()
       .awaitTermination())

process_silver_heartrate()

# COMMAND ----------

# MAGIC %md
# MAGIC We should see the same number of total records in our silver table as the deduplicated count above, and a small percentage of these will correctly be flagged with "Negative BPM".

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM heart_rate_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM heart_rate_silver
# MAGIC WHERE bpm_check = "Negative BPM"

# COMMAND ----------

# MAGIC %md
# MAGIC Now land a new batch of data and propagate changes through bronze into the silver table.

# COMMAND ----------

new_batch()
process_silver_heartrate()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM heart_rate_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>

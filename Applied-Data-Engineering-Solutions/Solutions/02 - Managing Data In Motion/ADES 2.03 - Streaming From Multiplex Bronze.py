# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming from Multiplex Bronze
# MAGIC 
# MAGIC In this notebook, you will configure a query to consume and parse raw data from a single topic as it lands in the multiplex bronze table configured in the last lesson. We'll continue refining this query in the following notebooks.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_heartrate_silver.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Describe how filters are applied to streaming jobs
# MAGIC - Use built-in functions to flatten nested JSON data
# MAGIC - Parse and save binary-encoded strings to native types

# COMMAND ----------

# MAGIC %md
# MAGIC Declare database and set all path variables.

# COMMAND ----------

# MAGIC %run ../Includes/silver-setup

# COMMAND ----------

# MAGIC %md
# MAGIC Use the following cell to reset the target directories, if necessary.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS heart_rate_silver")
dbutils.fs.rm(Paths.silverRecordingsTable, True)
dbutils.fs.rm(Paths.silverRecordingsCheckpoint, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Batch Read
# MAGIC 
# MAGIC Before building our streams, we'll start with a static view of our data. Working with static data can be easier during interactive development as no streams will be triggered. Because we're working with Delta Lake as our source, we'll still get the most up-to-date version of our table each time we execute a query.
# MAGIC 
# MAGIC If you're working with SQL, you can just directly query the table registered in the previous lesson `bronze`. Python and Scala users can easily create a Dataframe from a registered table.

# COMMAND ----------

batchDF = spark.table("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake stores our schema information. Let's print it out, just to make sure we remember.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Preview your data.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC There are multiple topics being ingested. So, we'll need to define logic for each of these topics separately.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC We'll cast our binary fields as strings, as this will allow us to manually review their contents.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Heart Rate Recordings
# MAGIC 
# MAGIC Let's start by defining logic to parse our heart rate recordings. We'll write this logic against our static data. Note that there are some [unsupported operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations) in Structured Streaming, so we may need to refactor some of our logic if we don't build our current queries with these limitations in mind.
# MAGIC 
# MAGIC Together, we'll iteratively develop a single query that parses our `bpm` topic to the following schema.
# MAGIC 
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | device_id | LONG | 
# MAGIC | time | TIMESTAMP | 
# MAGIC | heartrate | DOUBLE |
# MAGIC 
# MAGIC We'll be creating the table `heartrate_silver` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_heartrate_silver.png" width="60%" />

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "device_id LONG, time TIMESTAMP, heartrate DOUBLE") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "bpm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Logic for Streaming Read
# MAGIC 
# MAGIC We can define a streaming read directly against our Delta table. Note that most configuration for streaming queries is done on write rather than read, so here we see little change to our above logic.
# MAGIC 
# MAGIC The cell below shows how to convert a static table into a streaming temp view (if you wish to write streaming queries with Spark SQL).

# COMMAND ----------

(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("TEMP_bronze")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Updating our above query to refer to this temp view gives us a streaming result.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC  v.*
# MAGIC FROM
# MAGIC  (
# MAGIC    SELECT
# MAGIC      from_json(
# MAGIC        cast(value AS STRING),
# MAGIC        "device_id LONG, time TIMESTAMP, heartrate DOUBLE"
# MAGIC      ) v
# MAGIC    FROM
# MAGIC      TEMP_bronze
# MAGIC    WHERE
# MAGIC      topic = "bpm"
# MAGIC  )

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below has this logic refactored to Python.

# COMMAND ----------

bpmDF = (spark.readStream
  .table("bronze")
  .filter("topic = 'bpm'")
  .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
  .select("v.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that anytime a streaming read is displayed to a notebook, a streaming job will begin. To persist results to disk, a streaming write will need to be performed.
# MAGIC 
# MAGIC Using the `trigger(once=True)` option will process all records as a single batch.

# COMMAND ----------

(bpmDF.writeStream
    .option("checkpointLocation", Paths.silverRecordingsCheckpoint)
    .option("path", Paths.silverRecordingsTable)
    .trigger(once=True)
    .table("heart_rate_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> Before continuing, make sure you cancel any streams. The `Run All` button at the top of the screen will say `Stop Execution` if you have a stream still running. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Motivations
# MAGIC 
# MAGIC In addtion to parsing records and flattening and changing our schema, we should also check the quality of our data before writing to our silver tables.
# MAGIC 
# MAGIC In the following notebooks, we'll review various quality checks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

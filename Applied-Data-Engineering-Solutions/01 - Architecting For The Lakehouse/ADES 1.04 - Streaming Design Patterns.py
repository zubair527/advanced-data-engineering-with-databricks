# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Design Patterns
# MAGIC 
# MAGIC The Lakehouse has been designed from the beginning to work seamlessly with datasets that grow infinitely over time. While Spark Structured Streaming is often positioned as a near real-time data processing solution, it combines with Delta Lake to also provide easy batch processing of incremental data while drastically simplifying the overhead required to track data changes over time.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, student will be able to:
# MAGIC - Use Structured Streaming to complete simple incremental ETL
# MAGIC - Perform incremental writes to multiple tables
# MAGIC - Incrementally update values in a key value store
# MAGIC - Process Change Data Capture (CDC) data into Delta Tables using `MERGE`
# MAGIC - Join two incremental tables
# MAGIC - Join incremental and batch tables

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook.

# COMMAND ----------

# MAGIC %run ../Includes/sql-setup $course="stream_design" $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC Note that because Structured Streaming will be used throughout this lesson, checkpoint directories will need to be specified for each of our different streaming queries.
# MAGIC 
# MAGIC The code below declares the checkpoints used throughout the lesson, and does a recursive delete to remove any state information from previous runs.

# COMMAND ----------

checkpointPath = userhome + "/_checkpoints/"
silverCheckpoint = checkpointPath + "silver/"
splitStreamCheckpoint = checkpointPath + "split_stream/"
keyValueCheckpoint = checkpointPath + "key_value/"
silverStatusCheckpoint = checkpointPath + "silver_status/"
joinedCheckpoint = checkpointPath + "joined/"
joinStatusCheckpoint = checkpointPath + "join_status/"

dbutils.fs.rm(checkpointPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple Incremental ETL
# MAGIC 
# MAGIC Likely the highest volume of data being processed by most organizations could largely be describing as moving data from one location to another while applying light transformations and validations. As most source data continues to grow as time passes, it's appropriate to refer to this data as incremental (sometimes also referred to as streaming data). Structured Streaming and Delta Lake make incremental ETL easy. 
# MAGIC 
# MAGIC Below we'll create a simple table and insert some values.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE bronze 
# MAGIC (id INT, name STRING, value DOUBLE); 
# MAGIC 
# MAGIC INSERT INTO bronze
# MAGIC VALUES (1, "Yve", 1.0),
# MAGIC   (2, "Omar", 2.5),
# MAGIC   (3, "Elia", 3.3)

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell defines an incremental read on the table just created using Structured Streaming, adds a field to capture when the record was processed, and writes out to a new table as a single batch.

# COMMAND ----------

def update_silver():
    spark.readStream.table("bronze").withColumn("processed_time", F.current_timestamp()).writeStream.option("checkpointLocation", silverCheckpoint).trigger(once=True).table("silver")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that while this code uses Structured Streaming, it's appropriate to think of this as a triggered batch processing incremental changes.

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, the stream runs for a very brief time, and the `silver` table written contains all the values previously written to `bronze`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver

# COMMAND ----------

# MAGIC %md
# MAGIC Processing new records is as easy as adding them to our source table `bronze`...

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (4, "Ted", 4.7),
# MAGIC   (5, "Tiffany", 5.5),
# MAGIC   (6, "Vini", 6.3)

# COMMAND ----------

# MAGIC %md
# MAGIC ... and re-executing the incremental batch processing code.

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake is ideally suited for easily tracking and propagating inserted data through a series of tables. This pattern has a number of names, including "medallion", "multi-hop", "Delta", and "bronze/silver/gold" architecture.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Multiple Tables
# MAGIC 
# MAGIC Those familiar with Structured Streaming may be aware that the `foreachBatch` method provides the option to execute custom data writing logic on each microbatch of streaming data.
# MAGIC 
# MAGIC New DBR functionality provides guarantees that these writes will be idempotent, even when writing to multiple tables. This is especially useful when data for multiple tables might be contained within a single record.
# MAGIC 
# MAGIC The code below first defines the custom writer logic to append records to two new tables, and then demonstrates using this function within `foreachBatch`.

# COMMAND ----------

def write_twice(microBatchDF, batchId):
    appId = 'write_twice'
    
    microBatchDF.select("id", "name", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_name")
    
    microBatchDF.select("id", "value", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_value")


def split_stream():
    (spark.readStream.table("bronze")
        .writeStream
        .foreachBatch(write_twice)
        .outputMode("update")
        .option("checkpointLocation", splitStreamCheckpoint)
        .trigger(once=True)
        .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Note that while a stream will again be triggered, the two writes contained within the `write_twice` function are using Spark batch syntax. This will always be the case for writers called by `foreachBatch`.

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC The cells below demonstrate the logic was applied properly to split the initial data into two tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_name

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the `processed_time` for each of these tables differs slightly. The logic defined above captures the current timestamp at the time each write executes, demonstrating that while both writes happen within the same streaming microbatch process, they are fully independent transactions (as such, downstream logic should be tolerant for slightly asynchronous updates).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_value

# COMMAND ----------

# MAGIC %md
# MAGIC Insert more values into the `bronze` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (7, "Viktor", 7.4),
# MAGIC   (8, "Hiro", 8.2),
# MAGIC   (9, "Shana", 9.9)

# COMMAND ----------

# MAGIC %md
# MAGIC And we can now pick up these new records and write to two tables.

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, only new values are inserted into the two tables, again a few moments apart.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Aggregates in a Key-Value Store
# MAGIC 
# MAGIC Incremental aggregation can be useful for a number of purposes, including dashboarding and enriching reports with current summary data.
# MAGIC 
# MAGIC The logic below defines a handful of aggregations against the `silver` table.

# COMMAND ----------

def update_key_value():
    (spark.readStream
         .table("silver")
         .groupBy("id")
         .agg(F.sum("value").alias("total_value"), 
              F.mean("value").alias("avg_value"),
              F.count("value").alias("record_count"))
         .writeStream
         .option("checkpointLocation", keyValueCheckpoint)
         .outputMode("complete")
         .trigger(once=True)
         .table("key_value"))

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**: Because the transformations above require shuffling data, setting the number of partitions to map to the cores in our streaming cluster will provide more efficient performance. (If the cluster size will be scaled up for production, the maximum number of cores that will be present in the cluster should be used when configuring this setting.)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 4)

# COMMAND ----------

update_key_value()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM key_value

# COMMAND ----------

# MAGIC %md
# MAGIC Adding more values to the `silver` table will allow more interesting aggregation.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver
# MAGIC VALUES (1, "Yve", 1.0, current_timestamp()),
# MAGIC   (2, "Omar", 2.5, current_timestamp()),
# MAGIC   (3, "Elia", 3.3, current_timestamp()),
# MAGIC   (7, "Viktor", 7.4, current_timestamp()),
# MAGIC   (8, "Hiro", 8.2, current_timestamp()),
# MAGIC   (9, "Shana", 9.9, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC One thing to note is that the logic being executed is currently overwriting the resulting table with each write. In the next section, `MERGE` will be used in combination with `foreachBatch` to update existing records. This pattern can also be applied with key-value stores.

# COMMAND ----------

update_key_value()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM key_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Change Data Capture Data
# MAGIC While the change data capture (CDC) data emitted by various systems will vary greatly, incrementally processing these data with Databricks is straightforward.
# MAGIC 
# MAGIC Here the `bronze_status` table will represent the raw CDC information, rather than row-level data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze_status 
# MAGIC (user_id INT, status STRING, update_type STRING, processed_timestamp TIMESTAMP);
# MAGIC 
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (1, "new", "insert", current_timestamp()),
# MAGIC         (2, "repeat", "update", current_timestamp()),
# MAGIC         (3, "at risk", "update", current_timestamp()),
# MAGIC         (4, "churned", "update", current_timestamp()),
# MAGIC         (5, null, "delete", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC The `silver_status` table below has been created to track the current `status` for a given `user_id`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver_status (user_id INT, status STRING, updated_timestamp TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC The `MERGE` statement can easily be written with SQL to apply CDC changes appropriately, given the type of update received.
# MAGIC 
# MAGIC The rest of the `upsert_cdc` method contains the logic necessary to run SQL code against a micro-batch in a PySpark DataStreamWriter.

# COMMAND ----------

def upsert_cdc(microBatchDF, batchID):
    microBatchDF.createTempView("bronze_batch")
    
    query = """
        MERGE INTO silver_status s
        USING bronze_batch b
        ON b.user_id = s.user_id
        WHEN MATCHED AND b.update_type = "update"
          THEN UPDATE SET user_id=b.user_id, status=b.status, updated_timestamp=b.processed_timestamp
        WHEN MATCHED AND b.update_type = "delete"
          THEN DELETE
        WHEN NOT MATCHED AND b.update_type = "update" OR b.update_type = "insert"
          THEN INSERT (user_id, status, updated_timestamp)
          VALUES (b.user_id, b.status, b.processed_timestamp)
    """
    
    microBatchDF._jdf.sparkSession().sql(query)
    
def streaming_merge():
    spark.readStream.table("bronze_status").writeStream.foreachBatch(upsert_cdc).option("checkpointLocation", silverStatusCheckpoint).outputMode("update").trigger(once=True).start()

# COMMAND ----------

# MAGIC %md
# MAGIC As always, we incrementally process newly arriving records.

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status

# COMMAND ----------

# MAGIC %md
# MAGIC Inserting new records will allow us to then apply these changes to our silver data.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (1, "repeat", "update", current_timestamp()),
# MAGIC         (2, "at risk", "update", current_timestamp()),
# MAGIC         (3, "churned", "update", current_timestamp()),
# MAGIC         (4, null, "delete", current_timestamp()),
# MAGIC         (6, "new", "insert", current_timestamp())

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that at present, the logic would not be particularly robust to data arriving out-of-order or duplicate records (but these occurences can be handled).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Two Incremental Tables
# MAGIC 
# MAGIC Note that there are many intricacies around watermarking and windows when dealing with incremental joins, and that not all join types are supported.

# COMMAND ----------

def stream_stream_join():
    nameDF = spark.readStream.table("silver_name")
    valueDF = spark.readStream.table("silver_value")
    
    (nameDF.join(valueDF, nameDF.id == valueDF.id, "inner")
        .select(nameDF.id, 
                nameDF.name, 
                valueDF.value, 
                F.current_timestamp().alias("joined_timestamp"))
        .writeStream
        .option("checkpointLocation", joinedCheckpoint)
        .table("joined_streams"))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the logic defined above does not set a `trigger` option. This means that the stream will run in continuous execution mode, triggering every 500ms by default.

# COMMAND ----------

stream_stream_join()

# COMMAND ----------

# MAGIC %md
# MAGIC Running `display()` on a streaming table read is a way to monitor table updates in near-real-time while in interactive development. Note that a separate stream is started.

# COMMAND ----------

display(spark.readStream.table("joined_streams"))

# COMMAND ----------

# MAGIC %md
# MAGIC Here we'll add new values to the `bronze` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (10, "Pedro", 10.5),
# MAGIC   (11, "Amelia", 11.5),
# MAGIC   (12, "Diya", 12.3),
# MAGIC   (13, "Li", 13.4),
# MAGIC   (14, "Daiyu", 14.2),
# MAGIC   (15, "Jacques", 15.9)

# COMMAND ----------

# MAGIC %md
# MAGIC The stream-stream join is configured against the tables resulting from the `split_stream` function; run this again and data should quickly process through the streaming join running above.

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC Interactive streams should always be stopped before leaving a notebook session, as they can keep clusters from timing out and incur unnecessary cloud costs.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Incremental and Static Data
# MAGIC 
# MAGIC While incremental tables are ever-appending, static tables typically can be thought of as containing data that may be changed or overwritten.
# MAGIC 
# MAGIC Because of Delta Lake's transactional guarantees and caching, Databricks ensures that each microbatch of streaming data that's joined back to a static table will contain the current version of data from the static table.

# COMMAND ----------

statusDF = spark.read.table("silver_status")
bronzeDF = spark.readStream.table("bronze")

bronzeDF.alias("bronze").join(statusDF.alias("status"), bronzeDF.id==statusDF.user_id, "inner").select("bronze.*", "status.status").writeStream.option("checkpointLocation", joinStatusCheckpoint).table("joined_status")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status

# COMMAND ----------

# MAGIC %md
# MAGIC Only those records with a matching `id` in `joined_status` at the time the stream is processed will be represented in the resulting table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status

# COMMAND ----------

# MAGIC %md
# MAGIC Processing new records into the `silver_status` table will not automatically trigger updates to the results of the stream-static join.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (11, "repeat", "update", current_timestamp()),
# MAGIC         (12, "at risk", "update", current_timestamp()),
# MAGIC         (16, "new", "insert", current_timestamp()),
# MAGIC         (17, "repeat", "update", current_timestamp())

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status

# COMMAND ----------

# MAGIC %md
# MAGIC Only new data appearing on the streaming side of the query will trigger records to process using this pattern.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (16, "Marissa", 1.9),
# MAGIC   (17, "Anne", 2.7)

# COMMAND ----------

# MAGIC %md
# MAGIC The incremental data in a stream-static join "drives" the stream, guaranteeing that each microbatch of data joins with the current values present in the valid version of the static table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop Streaming Jobs

# COMMAND ----------

# Stop Streaming Job
for stream in spark.streams.active:
        stopped = True
        stream.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

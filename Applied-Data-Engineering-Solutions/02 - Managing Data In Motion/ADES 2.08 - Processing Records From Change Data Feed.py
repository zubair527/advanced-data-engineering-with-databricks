# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Processing Records from Delta Change Data Feed
# MAGIC 
# MAGIC In this notebook, we'll demonstrate an end-to-end of how you can easily propagate changes through a Lakehouse with Delta Lake Change Data Feed (CDF).
# MAGIC 
# MAGIC For this demo, we'll work with a slightly different dataset representing patient information for medical records. Descriptions of the data at various stages follow.
# MAGIC 
# MAGIC ### Bronze Table
# MAGIC Here we store all records as consumed. A row represents:
# MAGIC 1. A new patient providing data for the first time
# MAGIC 1. An existing patient confirming that their information is still correct
# MAGIC 1. An existing patient updating some of their information
# MAGIC 
# MAGIC The type of action a row represents is not captured.
# MAGIC 
# MAGIC ### Silver Table
# MAGIC This is the validated view of our data. Each patient will appear only once in this table. An upsert statement will be used to identify rows that have changed.
# MAGIC 
# MAGIC ### Gold Table
# MAGIC For this example, we'll create a simple gold table that captures patients that have a new address.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Enable Change Data Feed on a cluster or for a particular table
# MAGIC - Describe how changes are recorded
# MAGIC - Read CDF output with Spark SQL or PySpark
# MAGIC - Refactor ELT code to process CDF output

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC 
# MAGIC The following code defines some paths, a demo database, and clears out previous runs of the demo.
# MAGIC 
# MAGIC It also defines a variable `Raw` that we'll use to land raw data in our source directory, allowing us to process new records as if they were arriving in production.

# COMMAND ----------

# MAGIC %run ../Includes/cdc-setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC Enable CDF using Spark conf setting in a notebook or on a cluster will ensure it's used on all newly created Delta tables in that scope.

# COMMAND ----------

spark.conf.set('spark.databricks.delta.properties.defaults.enableChangeDataFeed',True)

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm source directory is empty.

# COMMAND ----------

try:
    dbutils.fs.ls(Raw.userdir)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Data with Auto Loader
# MAGIC 
# MAGIC Here we'll use Auto Loader to ingest data as it arrives.
# MAGIC 
# MAGIC The logic below is set up to either use trigger once to process all records loaded so far, or to continuously process records as they arrive.
# MAGIC 
# MAGIC We'll turn on continuous processing.

# COMMAND ----------

schema = "mrn BIGINT, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip BIGINT, city STRING, state STRING, updated timestamp"

bronzePath = f"{userhome}/bronze"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze
    (mrn BIGINT, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip BIGINT, city STRING, state STRING, updated timestamp) 
    LOCATION '{bronzePath}'
""")

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)
    .load(Raw.userdir)
    .writeStream
    .format("delta")
    .outputMode("append")
#     .trigger(once=True)
    .trigger(processingTime='5 seconds')
    .option("checkpointLocation", userhome + "/_bronze_checkpoint")
    .table("bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expand the stream monitor above to see the progress of your stream. No files should have been ingestedl.
# MAGIC 
# MAGIC Use the cell below to land a batch of data and list files in the source; you should see these records processed as a batch.

# COMMAND ----------

Raw.arrival()
dbutils.fs.ls(Raw.userdir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Target Table
# MAGIC 
# MAGIC Here we use `DEEP CLONE` to move read-only data from PROD to our DEV environment (where we have full write/delete access).

# COMMAND ----------

silverPath = userhome + "/silver"

spark.sql(f"""
    CREATE TABLE silver
    DEEP CLONE delta.`{URI}/pii/silver`
    LOCATION '{silverPath}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Tables that were not created with CDF enabled will not have it turned on by default, but can be altered to capture changes with the following syntax.
# MAGIC 
# MAGIC Note that editing properties will version a table. Also note that no CDC is captured during the CLONE operation above.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Data with Delta Lake
# MAGIC 
# MAGIC Here we define upsert logic into the silver table using a streaming read against the bronze table, matching on our unique identifier `mrn`.
# MAGIC 
# MAGIC We specify an additional conditional check to ensure that a field in the data has changed before inserting the new record.

# COMMAND ----------

def upsertToDelta(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("updates")
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO silver s
        USING updates u
        ON s.mrn = u.mrn
        WHEN MATCHED AND s.dob <> u.dob OR
                         s.sex <> u.sex OR
                         s.gender <> u.gender OR
                         s.first_name <> u.first_name OR
                         s.last_name <> u.last_name OR
                         s.street_address <> u.street_address OR
                         s.zip <> u.zip OR
                         s.city <> u.city OR
                         s.state <> u.state OR
                         s.updated <> u.updated
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)
    
(spark.readStream
    .table("bronze")
    .writeStream
    .foreachBatch(upsertToDelta)
    .outputMode("update")
#     .trigger(once=True)
    .trigger(processingTime='5 seconds')
    .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we have an additional metadata directory nested in our table directory, `_change_data`

# COMMAND ----------

dbutils.fs.ls(silverPath)

# COMMAND ----------

# MAGIC %md
# MAGIC We can see this directory also contains parquet files.

# COMMAND ----------

dbutils.fs.ls(silverPath + "/_change_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the Change Data Feed
# MAGIC 
# MAGIC To pick up the recorded CDC data, we add two options:
# MAGIC - `readChangeData`
# MAGIC - `startingVersion` (can use `startingTimestamp` instead)
# MAGIC 
# MAGIC Here we'll do a streaming display of just those patients in LA. Note that users with changes have two records present.

# COMMAND ----------

cdcDF = spark.readStream.format("delta").option("readChangeData", True).option("startingVersion", 0).table("silver")
display(cdcDF.filter("city = 'Los Angeles'"))

# COMMAND ----------

# MAGIC %md
# MAGIC If we land another file in our source directory and wait a few seconds, we'll see that we now have captured CDC changes for multiple `_commit_version` (change the sort order of the `_commit_version` column in the display above to see this).

# COMMAND ----------

Raw.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table
# MAGIC Our gold table will capture all of those patients that have a new address, and record this information alongside 2 timestamps: the time at which this change was made in our source system (currently labeled `updated`) and the time this was processed into our silver table (captured by the `_commit_timestamp` generated CDC field).
# MAGIC 
# MAGIC Within silver table CDC records:
# MAGIC - check for max `_commit_version` for each record
# MAGIC - if new version and address change, insert to gold table
# MAGIC - record `updated_timestamp` and `processed_timestamp`
# MAGIC 
# MAGIC #### Gold Table Schema
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | new_street_address | string |
# MAGIC | new_zip | long |
# MAGIC | new_city | string |
# MAGIC | new_state | string |
# MAGIC | old_street_address | string |
# MAGIC | old_zip | long |
# MAGIC | old_city | string |
# MAGIC | old_state | string |
# MAGIC | updated_timestamp | timestamp |
# MAGIC | processed_timestamp | timestamp |

# COMMAND ----------

goldPath = userhome + "/gold"

spark.sql(f"""
    CREATE TABLE gold
        (mrn BIGINT,
        new_street_address STRING,
        new_zip BIGINT,
        new_city STRING,
        new_state STRING,
        old_street_address STRING,
        old_zip BIGINT,
        old_city STRING,
        old_state STRING,
        updated_timestamp TIMESTAMP,
        processed_timestamp TIMESTAMP)
    USING DELTA
    LOCATION '{goldPath}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note that we are using a table that has updates written to it as a streaming source! This is a **huge** value add, and something that historically has required exensively workarounds to process correctly.

# COMMAND ----------

silverStreamDF = spark.readStream.format("delta").option("readChangeData", True).option("startingVersion", 0).table("silver")

# COMMAND ----------

# MAGIC %md
# MAGIC Our `_change_type` field lets us easily distinguish valid and invalid records.
# MAGIC 
# MAGIC New valid rows will have the `update_postimage` or `insert` label.
# MAGIC New invalid rows will have the `update_preimage` or `delete` label. 
# MAGIC 
# MAGIC (**NOTE**: We'll demonstrate logic for propagating deletes a little later)
# MAGIC 
# MAGIC In the cell below, we'll define two queries against our streaming source to perform a stream-stream merge on our data.

# COMMAND ----------

newDF = (silverStreamDF.filter(F.col("_change_type").isin(["update_postimage", "insert"]))
             .selectExpr("mrn",
                 "street_address new_street_address",
                 "zip new_zip",
                 "city new_city",
                 "state new_state",
                 "updated updated_timestamp",
                 "_commit_timestamp processed_timestamp"))

                                                                                         
oldDF = (silverStreamDF.filter(F.col("_change_type").isin(["update_preimage"]))
             .selectExpr("mrn",
                 "street_address old_street_address",
                 "zip old_zip",
                 "city old_city",
                 "state old_state",
                 "_commit_timestamp processed_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Assuming that we have properly deduplicated our data to ensure that only a single record for our `mrn` can be processed to our silver table, `mrn` and `_commit_timestamp` (aliased to `processed_timestamp` here) serve as a unique composite key.
# MAGIC 
# MAGIC Our join will allow us to match up the current and previous states of our data to track all changes.
# MAGIC 
# MAGIC This table could drive further downstream processes, such as triggering confirmation emails or automatic mailings for patients with updated addresses.
# MAGIC 
# MAGIC Our CDC data arrives as a stream, so only newly changed data at the silver level will be processed. Therefore, we can write to our gold table in append mode and maintain the grain of our data.

# COMMAND ----------

(newDF.withWatermark("processed_timestamp", "3 minutes")
    .join(oldDF, ["mrn", "processed_timestamp"], "left")
    .filter("new_street_address <> old_street_address OR old_street_address IS NULL")
    .writeStream
    .outputMode("append")
#     .trigger(once=True)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", userhome + "/_gold_checkpoint")
    .table("gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC Note the number of rows in our gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold

# COMMAND ----------

# MAGIC %md
# MAGIC If we land a new raw file and wait a few seconds, we can see that all of our changes have propagated through our pipeline.
# MAGIC 
# MAGIC (This assumes you're using `processingTime` instead of trigger once processing. Scroll up to the gold table streaming write to wait for a new peak in the processing rate to know your data has arrived.)

# COMMAND ----------

Raw.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC You should be able to see a jump in the number of records in your gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure to run the following cell to stop all active streams.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Propagating Deletes
# MAGIC 
# MAGIC While some use cases may require processing deletes alongside updates and inserts, the most important delete requests are those that allow companies to maintain compliance with privacy regulations such as GDPR and CCPA. Most companies have stated SLAs around how long these requests will take to process, but for various reasons, these are often handled in pipelines separate from their core ETL.
# MAGIC 
# MAGIC Here, we should a single user being deleted from our `silver` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver WHERE mrn = 14125426

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, when we try to locate this user in our `silver` table, we'll get no result.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver WHERE mrn = 14125426

# COMMAND ----------

# MAGIC %md
# MAGIC This change has been captured in our Change Data Feed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver", 0)
# MAGIC WHERE mrn = 14125426

# COMMAND ----------

# MAGIC %md
# MAGIC Because we have a record of this delete action, we can define logic that propagates deletes to our `gold` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deletes AS (
# MAGIC   SELECT mrn
# MAGIC   FROM table_changes("silver", 0)
# MAGIC   WHERE _change_type='delete'
# MAGIC )
# MAGIC 
# MAGIC MERGE INTO gold g
# MAGIC USING deletes d
# MAGIC ON d.mrn=g.mrn
# MAGIC WHEN MATCHED
# MAGIC   THEN DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC This drastically simplifies deleting user data, and allows the keys and logic used in your ETL to also be used for propagating delete requests.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold WHERE mrn = 14125426

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

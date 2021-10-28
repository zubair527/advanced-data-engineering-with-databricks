# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Load Data to Multiplex Bronze
# MAGIC 
# MAGIC Our chief architect has decided that rather than connecting directly to Kafka, we'll land raw records as JSON files in cloud object storage and ingest them with Auto Loader. We'll build a multiplex table that ingests and stores the entire history of this incremental feed. The initial table will store data from all of our topics and have the following schema.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | key | BINARY |
# MAGIC | value | BINARY |
# MAGIC | topic | STRING |
# MAGIC | partition | LONG |
# MAGIC | offset | LONG
# MAGIC | timestamp | LONG |
# MAGIC | date | DATE |
# MAGIC | week_part | STRING |
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />
# MAGIC 
# MAGIC **NOTE**: Details on additional configurations for connecting to Kafka are available [here](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html).
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Describe a multiplex design
# MAGIC - Apply Auto Loader to incrementally process records
# MAGIC - Configure trigger intervals
# MAGIC - Use `trigger once` logic to execute triggered incremental loading of data.

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell declares the paths needed throughout this notebook.

# COMMAND ----------

# MAGIC %run ../Includes/bronze-setup

# COMMAND ----------

# MAGIC %md
# MAGIC The `Paths` variable will be declared in each notebook for easy file management.
# MAGIC 
# MAGIC **NOTE**: All records are being stored on the DBFS root for this training example. Setting up separate databases and storage accounts for different layers of data is preferred in both development and production.

# COMMAND ----------

Paths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Source Data
# MAGIC 
# MAGIC Data files are being written to the path specified by the variable below.
# MAGIC 
# MAGIC Use the following cell to examine the schema in the source data and determine if anything needs to be changed as it's being ingested.

# COMMAND ----------

# TODO
Paths.sourceDaily

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data to Join with Date Lookup Table
# MAGIC The initialization script has loaded a `date_lookup` table. This table has a number of pre-computed date values. Note that additional fields indicating holidays or financial quarters might often be added to this table for later data enrichment.
# MAGIC 
# MAGIC Pre-computing and storing these values is especially important based on our desire to partition our data by year and week, using the string pattern `YYYY-WW`. While Spark has both `year` and `weekofyear` functions built in, the `weekofyear` function may not provide expected behavior for dates falling in the last week of December or [first week of January](https://spark.apache.org/docs/2.3.0/api/sql/#weekofyear), as it defines week 1 as the first week with >3 days.
# MAGIC 
# MAGIC While this edge case is esoteric to Spark, a `date_lookup` table that will be used across the organization is important for making sure that data is consistently enriched with date-related details.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE date_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC The `date_lookup` table is very small (here we only include date info for 3 years); manually caching the subset of this table we'll be using before proceeding will make sure it's readily available in memory, although the Delta Cache will automatically cache and reuse this data anyway.
# MAGIC 
# MAGIC The current table being implemented requires that we capture the accurate `week_part` for each date.
# MAGIC 
# MAGIC The cell below loads and caches these two fields.

# COMMAND ----------

dateLookup = spark.table("date_lookup").select("date", "week_part")
dateLookup.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Working with the JSON data stored in the `Paths.sourceDaily` location, transform the `timestamp` column as necessary to match to join it with the `date` column.

# COMMAND ----------

# TODO
jsonDF = spark.read.json(Paths.sourceDaily)
 
joinedDF = (jsonDF.join(F.broadcast(dateLookup),
#     <INSERT-MATCHING-CONDITION>,
    "left"))
 
display(joinedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Triggered Incremental Auto Loading to Multiplex Bronze Table
# MAGIC 
# MAGIC Below is starter code for a function to incrementally process data from the source directory to the bronze table, creating the table during the initial write.
# MAGIC 
# MAGIC Fill in the missing code to:
# MAGIC - Define the schema
# MAGIC - Configure Auto Loader to use the JSON format and specified schema
# MAGIC - Perform a broadcast join with the date_lookup table
# MAGIC - Partition the data by the `topic` and `week_part` fields

# COMMAND ----------

# TODO
def process_bronze():
#     schema = "<FILL-IN>"
     
    (spark.readStream
#         <FILL-IN>
        .load(Paths.sourceDaily)
#         .join(<FILL-IN>)
        .writeStream
        .option("checkpointLocation", Paths.bronzeCheckpoint)
#         .partitionBy(<FILL-IN>)
        .option("path", Paths.bronzeTable)
        .trigger(once=True)
        .table("bronze")
        .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to process an incremental batch of data.

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC Review the count of processed records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Preview the data to ensure records are being ingested correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC The `Raw.arrival()` code below is a helper class to land new data in the source directory.
# MAGIC 
# MAGIC Executing the following cell should successfully process a new batch.

# COMMAND ----------

Raw.arrival()
process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the count is now higher.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

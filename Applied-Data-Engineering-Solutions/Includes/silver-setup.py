# Databricks notebook source
# MAGIC %run ./new-data

# COMMAND ----------

def process_bronze():
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
    
    (spark.readStream
        .format("cloudFiles")
        .schema(schema)
        .option("cloudFiles.format", "json")
        .load(Paths.sourceDaily)
        .join(F.broadcast(spark.table("date_lookup").select("date", "week_part")), F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
        .writeStream
        .option("checkpointLocation", Paths.bronzeCheckpoint)
        .partitionBy("topic", "week_part")
        .option("path", Paths.bronzeTable)
        .trigger(once=True)
        .table("bronze")
        .awaitTermination())

# COMMAND ----------

def new_batch():
    Raw.arrival()
    process_bronze()


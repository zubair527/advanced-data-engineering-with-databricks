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

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS workouts_silver
  (user_id INT, workout_id INT, time TIMESTAMP, action STRING, session_id INT)
  USING DELTA
  LOCATION '{Paths.silverWorkoutsTable}'
""")

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsertToDelta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.query)
        
def workouts_silver(once=False, processing_time="15 seconds"):
    
    query = """
        MERGE INTO workouts_silver a
        USING workout_updates b
        ON a.user_id=b.user_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """

    streamingMerge=Upsert(query, "workout_updates")
    
    (spark.readStream
        .option("ignoreDeletes", True)
        .table("bronze")
        .filter("topic = 'workout'")
        .select(F.from_json(F.col("value").cast("string"), "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT").alias("v"))
        .select("v.*")
        .select("user_id", "workout_id", F.col("timestamp").cast("timestamp").alias("time"), "action", "session_id")
        .withWatermark("time", "30 seconds")
        .dropDuplicates(["user_id", "time"])
        .writeStream
        .foreachBatch(streamingMerge.upsertToDelta)
        .outputMode("update")
        .option("checkpointLocation", Paths.silverWorkoutsCheckpoint)
        .queryName("workouts_silver")
        .trigger(once=True)
        .start()
        .awaitTermination())

# COMMAND ----------

def process_silver_workouts():
    Raw.arrival()
    process_bronze()
    workouts_silver()


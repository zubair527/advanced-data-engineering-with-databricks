# Databricks notebook source
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

# COMMAND ----------

# MAGIC %run ./new-data

# COMMAND ----------

# MAGIC %run ./build-pii-pipeline

# COMMAND ----------

# MAGIC %run ./build-gym-mac-log

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


def process_silver_heartrate():
    query = """
        MERGE INTO heart_rate_silver a
        USING heart_rate_updates b
        ON a.device_id=b.device_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
    """

    streamingMerge=Upsert(query, "heart_rate_updates")
    
    (spark.readStream
        .table("bronze")
        .filter("topic = 'bpm'")
        .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
        .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check"))
        .withWatermark("time", "30 seconds")
        .dropDuplicates(["device_id", "time"]).writeStream
        .foreachBatch(streamingMerge.upsertToDelta)
        .outputMode("update")
        .option("checkpointLocation", Paths.silverRecordingsCheckpoint)
        .trigger(once=True)
        .start()
        .awaitTermination())    
    
def process_completed_workouts():
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW TEMP_completed_workouts AS (
          SELECT a.user_id, a.workout_id, a.session_id, a.start_time start_time, b.end_time end_time, a.in_progress AND (b.in_progress IS NULL) in_progress
          FROM (
            SELECT user_id, workout_id, session_id, time start_time, null end_time, true in_progress
            FROM workouts_silver
            WHERE action = "start") a
          LEFT JOIN (
            SELECT user_id, workout_id, session_id, null start_time, time end_time, false in_progress
            FROM workouts_silver
            WHERE action = "stop") b
          ON a.user_id = b.user_id AND a.session_id = b.session_id
        )
    """)
    
    (spark.table("TEMP_completed_workouts").write
        .mode("overwrite")
        .saveAsTable("completed_workouts"))

# COMMAND ----------

def process_workout_bpm():
    spark.readStream.table("heart_rate_silver").createOrReplaceTempView("TEMP_heart_rate_silver")
    
    spark.sql("""
        SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
        FROM TEMP_heart_rate_silver c
        INNER JOIN (
          SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
          FROM completed_workouts a
          INNER JOIN user_lookup b
          ON a.user_id = b.user_id) d
        ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
        WHERE c.bpm_check = 'OK'""").createOrReplaceTempView("TEMP_workout_bpm")
    
    (spark.table("TEMP_workout_bpm")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", Paths.workoutBpmCheckpoint)
        .option("path", Paths.workoutBpm)
        .trigger(once=True)
        .table("workout_bpm")
        .awaitTermination())

# COMMAND ----------

def process_gold_sources():
    Raw.arrival()
    process_bronze()
    workouts_silver()
    process_silver_heartrate()
    process_completed_workouts()
    process_workout_bpm()


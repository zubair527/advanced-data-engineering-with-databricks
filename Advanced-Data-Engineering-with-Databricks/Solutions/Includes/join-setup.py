# Databricks notebook source
# MAGIC %run ./new-data

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "4")

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

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS heart_rate_silver
  (device_id LONG, time TIMESTAMP, heartrate DOUBLE, bpm_check STRING)
  USING DELTA
  LOCATION '{Paths.silverRecordingsTable}'
  """)

def process_silver_heartrate():
    query = """
        MERGE INTO heart_rate_silver a
        USING stream_updates b
        ON a.device_id=b.device_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
    """

    streamingMerge=Upsert(query)
    
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

spark.sql("DROP TABLE IF EXISTS users")
dbutils.fs.rm(Paths.users, True)
dbutils.fs.rm(Paths.usersCheckpointPath, True)

spark.sql("DROP TABLE IF EXISTS delete_requests")
dbutils.fs.rm(Paths.deleteRequests, True)

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS users
  (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP)
  USING DELTA
  LOCATION '{Paths.users}'
""")

spark.sql("DROP TABLE IF EXISTS user_lookup")
dbutils.fs.rm(Paths.userLookup, True)

spark.sql("DROP TABLE IF EXISTS user_bins")
dbutils.fs.rm(Paths.userBins, True)
    
def create_user_lookup():
    (spark.read
        .format("json")
        .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
        .load(f"{URI}/user-reg")
        .selectExpr(f"sha2(concat(user_id,'BEANS'), 256) AS alt_id", "device_id", "mac_address", "user_id")
        .write
        .option("path", Paths.userLookup)
        .mode("overwrite")
        .saveAsTable("user_lookup"))
    
from pyspark.sql.window import Window

window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())

def batch_rank_upsert(microBatchDF, batchId):
    appId = "batch_rank_upsert"
    
    (microBatchDF
        .filter(F.col("update_type").isin(["new", "update"]))
        .withColumn("rank", F.rank().over(window)).filter("rank == 1").drop("rank")
        .createOrReplaceTempView("ranked_updates"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING ranked_updates r
        ON u.alt_id=r.alt_id
        WHEN MATCHED AND u.updated < r.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

    (microBatchDF
         .filter("update_type = 'delete'")
         .select(
            "alt_id", 
            F.col("updated").alias("requested"), 
            F.date_add("updated", 30).alias("deadline"), 
            F.lit("requested").alias("status"))
        .write
        .format("delta")
        .mode("append")
        .option("txnVersion", batchId)
        .option("txnAppId", appId)
        .option("path", Paths.deleteRequests)
        .saveAsTable("delete_requests"))

def process_users():
    schema = """
        user_id LONG, 
        update_type STRING, 
        timestamp FLOAT, 
        dob STRING, 
        sex STRING, 
        gender STRING, 
        first_name STRING, 
        last_name STRING, 
        address STRUCT<
            street_address: STRING, 
            city: STRING, 
            state: STRING, 
            zip: INT
    >"""
    
    (spark.readStream
        .table("bronze")
        .filter("topic = 'user_info'")
        .dropDuplicates()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
        .select(F.sha2(F.concat(F.col("user_id"), F.lit("BEANS")), 256).alias("alt_id"),
            F.col('timestamp').cast("timestamp").alias("updated"),
            F.to_date('dob','MM/dd/yyyy').alias('dob'),
            'sex', 'gender','first_name','last_name',
            'address.*', "update_type")
        .writeStream
        .foreachBatch(batch_rank_upsert)
        .outputMode("update")
        .option("checkpointLocation", Paths.usersCheckpointPath)
        .trigger(once=True)
        .start()
        .awaitTermination())
    
def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
    return (F.when((age_col < 18), "under 18")
            .when((age_col >= 18) & (age_col < 25), "18-25")
            .when((age_col >= 25) & (age_col < 35), "25-35")
            .when((age_col >= 35) & (age_col < 45), "35-45")
            .when((age_col >= 45) & (age_col < 55), "45-55")
            .when((age_col >= 55) & (age_col < 65), "55-65")
            .when((age_col >= 65) & (age_col < 75), "65-75")
            .when((age_col >= 75) & (age_col < 85), "75-85")
            .when((age_col >= 85) & (age_col < 95), "85-95")
            .when((age_col >= 95), "95+")
            .otherwise("invalid age").alias("age"))

def build_user_bins():
    (spark.table("users")
         .join(
            spark.table("user_lookup")
                .select("alt_id", "user_id"), 
            ["alt_id"], 
            "left")
        .select("user_id", 
                age_bins(F.col("dob")),
                "gender", 
                "city", 
                "state")
         .write
        .format("delta")
        .option("path", Paths.userBins)
        .mode("overwrite")
        .saveAsTable("user_bins"))
    
create_user_lookup()
process_users()
build_user_bins()

# COMMAND ----------

def process_join_sources():
    Raw.arrival()
    process_bronze()
    workouts_silver()
    process_silver_heartrate()
    process_completed_workouts()




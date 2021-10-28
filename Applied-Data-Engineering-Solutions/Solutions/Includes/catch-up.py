# Databricks notebook source
# MAGIC %md
# MAGIC All notebooks from day 1 should run in any order once the `producer` has been started.
# MAGIC 
# MAGIC For later notebooks, this notebook can be used to declare all tables and load all data ingested through the `AutoLoader` and `COPY INTO` lessons.
# MAGIC 
# MAGIC To recreate all tables from a fresh start:
# MAGIC 1. Run all in the producer notebook
# MAGIC 1. Run all in this notebook
# MAGIC 1. Run all in the schedule_streaming_jobs notebook (pass the value `True` to the `once` widget)
# MAGIC 1. Run all in the schedule_batch_jobs notebook

# COMMAND ----------

# MAGIC %run ./ade-setup

# COMMAND ----------

# MAGIC %run ./table-declaration

# COMMAND ----------

# MAGIC %run ./gym-mac-log-prep

# COMMAND ----------

# register_users
spark.sql(f"""
COPY INTO registered_users
FROM '{URI}/user-reg'
FILEFORMAT = JSON
""")

# COMMAND ----------

# user_lookup
salt = "BEANS"

spark.sql(f"""
INSERT INTO user_lookup
SELECT sha2(concat(user_id,"{salt}"), 256) AS alt_id, device_id, mac_address, user_id
FROM registered_users
""")

# COMMAND ----------

# gym_mac_logs
def load_gym_logs():
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("first_timestamp DOUBLE, gym BIGINT, last_timestamp DOUBLE, mac STRING")
        .load(gym_mac_logs)
        .writeStream
        .format("delta")
        .option("checkpointLocation", Paths.gymMacLogsCheckpoint)
        .trigger(once=True)
        .start(Paths.gymMacLogs)
        .awaitTermination())
    
NewFile.arrival(continuous=True)
load_gym_logs()


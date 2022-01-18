# Databricks notebook source
# MAGIC %md
# MAGIC # Setting Up Tables
# MAGIC Managing database and table metadata, locations, and configurations at the beginning of project can help to increase data security, discoverability, and performance.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, students will be able to:
# MAGIC - Set database locations
# MAGIC - Specify database comments
# MAGIC - Set table locations
# MAGIC - Specify table comments
# MAGIC - Specify column comments
# MAGIC - Use table properties for custom tagging
# MAGIC - Explore table metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS djs_foo_db CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE djs_foo_db
# MAGIC COMMENT "This is a test database"
# MAGIC LOCATION "dbfs:/user/douglas.strodtman@databricks.com/foo_db"
# MAGIC WITH DBPROPERTIES (contains_pii = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED djs_foo_db

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE djs_foo_db.pii_test
# MAGIC (id INT, name STRING COMMENT "PII")
# MAGIC COMMENT "Contains PII"
# MAGIC TBLPROPERTIES ('contains_pii' = True) 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED djs_foo_db.pii_test

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE djs_foo_db.pii_test_2
# MAGIC (id INT, name STRING COMMENT "PII")
# MAGIC COMMENT "Contains PII"
# MAGIC LOCATION "dbfs:/user/douglas.strodtman@databricks.com/pii_test_2"
# MAGIC TBLPROPERTIES ('contains_pii' = True) 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED djs_foo_db.pii_test_2

# COMMAND ----------

# MAGIC %md
# MAGIC # ARCHIVED

# COMMAND ----------

# MAGIC %run ./ade-setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze_test (
# MAGIC key STRING,
# MAGIC offset BIGINT,
# MAGIC partition INT,
# MAGIC timestamp BIGINT,
# MAGIC topic STRING,
# MAGIC value STRING,
# MAGIC event_date DATE GENERATED ALWAYS AS (CAST(CAST(timestamp/1000 AS TIMESTAMP) AS DATE)))
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (topic, event_date)

# COMMAND ----------

(spark.readStream
    .schema("key STRING, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value STRING")
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(Paths.sourceDir)
    .writeStream
    .option("checkpointLocation", Paths.bronzeCheckpoint)
    .trigger(once=True)
    .table("bronze_test"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_test

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze_test

# COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse/ade_douglas_strodtman_databricks_com_db.db/bronze_test/topic=bpm")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS heart_rate_silver
  (device_id LONG, time TIMESTAMP, heartrate DOUBLE, bpm_check STRING)
  USING DELTA
  LOCATION '{Paths.silverRecordingsTable}'
""")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS workouts_silver
  (user_id INT, workout_id INT, time TIMESTAMP, action STRING, session_id INT)
  USING DELTA
  LOCATION '{Paths.silverWorkoutsTable}'
""")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS users
  (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP)
  USING DELTA
  LOCATION '{Paths.users}'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gym_mac_logs
    (first_timestamp DOUBLE, gym BIGINT, last_timestamp DOUBLE, mac STRING)
    USING delta
    LOCATION '{Paths.gymMacLogs}'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS completed_workouts
    (user_id INT, workout_id INT, session_id INT, start_time TIMESTAMP, end_time TIMESTAMP, valid BOOLEAN)
    USING DELTA
    LOCATION '{Paths.completedWorkouts}'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS workout_bpm
    (user_id INT, workout_id INT, session_id INT, time TIMESTAMP, heartrate DOUBLE)
    USING DELTA
    LOCATION '{Paths.workoutBpm}'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS user_bins
    (user_id BIGINT, age STRING, gender STRING, city STRING, state STRING)
    USING DELTA
    LOCATION '{Paths.userBins}'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS registered_users
    (device_id long, mac_address string, registration_timestamp double, user_id long)
    USING DELTA 
    LOCATION '{Paths.registeredUsers}'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS user_lookup
    (alt_id string, device_id long, mac_address string, user_id long)
    USING DELTA 
    LOCATION '{Paths.userLookup}'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS workout_bpm_summary
    (workout_id INT, session_id INT, user_id BIGINT, age STRING, gender STRING, city STRING, state STRING, min_bpm DOUBLE, avg_bpm DOUBLE, max_bpm DOUBLE, num_recordings BIGINT)
    USING DELTA 
    LOCATION '{Paths.workoutBpmSummary}'
""")


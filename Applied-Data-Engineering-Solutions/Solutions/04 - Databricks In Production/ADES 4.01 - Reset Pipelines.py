# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reset Pipelines
# MAGIC 
# MAGIC In this notebook, code is provided to remove all existing databases, data, and tables. Code is then provided to redeclare each table used in the architecture.

# COMMAND ----------

# MAGIC %run ../Includes/reset-&-install-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC We'll be using the `bronze_dev` table, which is a clone that already contains all of our daily data.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {database}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Code to declare all the other tables in our pipelines are provided below.

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS heart_rate_silver
  (device_id LONG, time TIMESTAMP, heartrate DOUBLE, bpm_check STRING)
  USING DELTA
  LOCATION '{Paths.silverRecordingsTable}'
""")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS workouts_silver
  (user_id INT, workout_id INT, time TIMESTAMP, action STRING, session_id INT)
  USING DELTA
  LOCATION '{Paths.silverWorkoutsTable}'
""")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS users
  (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP)
  USING DELTA
  LOCATION '{Paths.users}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gym_mac_logs
    (first_timestamp DOUBLE, gym BIGINT, last_timestamp DOUBLE, mac STRING)
    USING delta
    LOCATION '{Paths.gymMacLogs}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS completed_workouts
    (user_id INT, workout_id INT, session_id INT, start_time TIMESTAMP, end_time TIMESTAMP, in_progress BOOLEAN)
    USING DELTA
    LOCATION '{Paths.completedWorkouts}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS workout_bpm
    (user_id INT, workout_id INT, session_id INT, time TIMESTAMP, heartrate DOUBLE)
    USING DELTA
    LOCATION '{Paths.workoutBpm}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS user_bins
    (user_id BIGINT, age STRING, gender STRING, city STRING, state STRING)
    USING DELTA
    LOCATION '{Paths.userBins}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS registered_users
    (device_id long, mac_address string, registration_timestamp double, user_id long)
    USING DELTA 
    LOCATION '{Paths.registeredUsers}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS user_lookup
    (alt_id string, device_id long, mac_address string, user_id long)
    USING DELTA 
    LOCATION '{Paths.userLookup}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS workout_bpm_summary
    (workout_id INT, session_id INT, user_id BIGINT, age STRING, gender STRING, city STRING, state STRING, min_bpm DOUBLE, avg_bpm DOUBLE, max_bpm DOUBLE, num_recordings BIGINT)
    USING DELTA 
    LOCATION '{Paths.workoutBpmSummary}'
""")

spark.sql(f"""
    CREATE VIEW IF NOT EXISTS gym_user_stats AS (
    SELECT gym, mac_address, date, workouts, (last_timestamp - first_timestamp)/60 minutes_in_gym, (to_unix_timestamp(end_workout) - to_unix_timestamp(start_workout))/60 minutes_exercising
    FROM gym_mac_logs c
    INNER JOIN (
      SELECT b.mac_address, to_date(start_time) date, collect_set(workout_id) workouts, min(start_time) start_workout, max(end_time) end_workout
          FROM completed_workouts a
          INNER JOIN user_lookup b
          ON a.user_id = b.user_id
          GROUP BY mac_address, to_date(start_time)
      ) d
      ON c.mac = d.mac_address AND to_date(CAST(c.first_timestamp AS timestamp)) = d.date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC For this demo, we're only focused on processing those data coming through our multiplex bronze table, so we'll bypass the incremental loading for the `gym_mac_logs` and `user_lookup` tables and recreate the final results with a direct read of all files.

# COMMAND ----------

spark.read.json(f"{URI}/gym-logs/").write.option("path", Paths.gymMacLogs).mode("overwrite").saveAsTable("gym_mac_logs")

(spark.read
    .format("json")
    .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
    .load(f"{URI}/user-reg")
    .selectExpr(f"sha2(concat(user_id,'BEANS'), 256) AS alt_id", "device_id", "mac_address", "user_id")
    .write
    .option("path", Paths.userLookup)
    .mode("overwrite")
    .saveAsTable("user_lookup"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

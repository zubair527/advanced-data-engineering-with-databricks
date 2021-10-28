# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Fast Changing Facts with Stream-Static Join
# MAGIC 
# MAGIC In this lesson, you'll join streaming heart rate data with the completed workouts table.
# MAGIC 
# MAGIC We'll be creating the table `workout_bpm` in our architectural diagram.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_workout_bpm.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe guarantees around versioning and matching for stream-static joins
# MAGIC - Leverage Spark SQL and PySpark to process stream-static joins

# COMMAND ----------

# MAGIC %run ../Includes/join-setup

# COMMAND ----------

# MAGIC %md
# MAGIC A helper function has been defined to process a new batch of data to the source tables used in this lesson. (Note: this may take around 2 minutes.)

# COMMAND ----------

process_join_sources()

# COMMAND ----------

# MAGIC %md
# MAGIC Set up your streaming temp view. Note that we will only be streaming from **one** of our tables. The `completed_workouts` table is no longer streamable as it breaks the requirement of an ever-appending source for Structured Streaming. However, when performing a stream-static join with a Delta table, each batch will confirm that the newest version of the static Delta table is being used.

# COMMAND ----------

spark.readStream.table("heart_rate_silver").createOrReplaceTempView("TEMP_heart_rate_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Stream-Static Join to Align Workouts to Heart Rate Recordings
# MAGIC 
# MAGIC Below we'll configure our query to join our stream to our `completed_workouts` table.
# MAGIC 
# MAGIC Note that our heart rate recordings only have `device_id`, while our workouts use `user_id` as the unique identifier. We'll need to use our `user_lookup` table to match these values.
# MAGIC 
# MAGIC Importantly, our devices occasionally send messages with negative recordings, which represent a potential error in the recorded values. We'll need to define predicate conditions to ensure that only positive recordings are processed. 

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream in Append Mode
# MAGIC 
# MAGIC Below, we'll use our streaming temp view from above to insert new values into our `workout_bpm` table.

# COMMAND ----------

# MAGIC %md
# MAGIC Explore this results table below.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM workout_bpm

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM workout_bpm

# COMMAND ----------

# MAGIC %md
# MAGIC If desired, process another batch through all tables and update these results.

# COMMAND ----------

process_join_sources()
process_workout_bpm()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM workout_bpm

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

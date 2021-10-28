# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Materialized Gold Tables
# MAGIC 
# MAGIC Gold tables refer to highly refined, generally aggregate views of the data persisted to Delta Lake.
# MAGIC 
# MAGIC These tables are intended to drive core business logic, dashboards, and applications.
# MAGIC 
# MAGIC The necessity of gold tables will evolve over time; as more analysts and data scientists use your Lakehouse, analyzing query history will reveal trends in how data is queried, when, and by whom. Collaborating across teams, data engineers and platform admins can define SLAs to make highly valuable data available to teams in a timely fashion, all while cutting down the potential costs and latency associated with larger ad hoc queries.
# MAGIC 
# MAGIC In this notebook, we'll create a gold table that stores summary statistics about each completed workout alongside binned demographic information. In this way, our application can quickly populate statistics about how other users performed on the same workouts.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bpm_summary.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe performance differences between views and tables
# MAGIC - Implement a streaming aggregate table

# COMMAND ----------

# MAGIC %md
# MAGIC Set up path and checkpoint variables (these will be used later).

# COMMAND ----------

# MAGIC %run ../Includes/gold-setup

# COMMAND ----------

# MAGIC %md
# MAGIC A helper function has been defined to process a new batch of data to the source tables used in this lesson. (Note: this may take around 2 minutes.)

# COMMAND ----------

process_gold_sources()

# COMMAND ----------

# MAGIC %md
# MAGIC Recall that our `workout_bpm` table has already matched all completed workouts to user bpm recordings.
# MAGIC 
# MAGIC Explore this data below.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM workout_bpm
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Here we calculate some summary statistics for our workouts.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, workout_id, session_id, MIN(heartrate) min_bpm, MEAN(heartrate) avg_bpm, MAX(heartrate) max_bpm, COUNT(heartrate) num_recordings
# MAGIC FROM workout_bpm
# MAGIC GROUP BY user_id, workout_id, session_id

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can use our `user_lookup` table to match this back to our binned demographic information.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workout_id, session_id, a.user_id, age, gender, city, state, min_bpm, avg_bpm, max_bpm, num_recordings
# MAGIC FROM user_bins a
# MAGIC INNER JOIN
# MAGIC   (SELECT user_id, workout_id, session_id, MIN(heartrate) min_bpm, MEAN(heartrate) avg_bpm, MAX(heartrate) max_bpm, COUNT(heartrate) num_recordings
# MAGIC   FROM workout_bpm
# MAGIC   GROUP BY user_id, workout_id, session_id) b
# MAGIC ON a.user_id = b.user_id

# COMMAND ----------

# MAGIC %md
# MAGIC Because our `workout_bpm` table was written as an append-only stream, we can update our aggregation using a streaming job as well.

# COMMAND ----------

spark.readStream.table("workout_bpm").createOrReplaceTempView("TEMP_workout_bpm")

# COMMAND ----------

# MAGIC %md
# MAGIC Using trigger once logic with Delta Lake, we can ensure that we'll only calculate new results if records have changed in the upstream source tables.

# COMMAND ----------

(spark.sql("""
    SELECT workout_id, session_id, a.user_id, age, gender, city, state, min_bpm, avg_bpm, max_bpm, num_recordings
    FROM user_bins a
    INNER JOIN
      (SELECT user_id, workout_id, session_id, MIN(heartrate) min_bpm, MEAN(heartrate) avg_bpm, MAX(heartrate) max_bpm, COUNT(heartrate) num_recordings
      FROM TEMP_workout_bpm
      GROUP BY user_id, workout_id, session_id) b
    ON a.user_id = b.user_id"""
    ).writeStream
        .format("delta")
        .option("path", Paths.workoutBpmSummary)
        .option("checkpointLocation", Paths.workoutBpmSummaryCheckpoint)
        .outputMode("complete")
        .trigger(once=True)
        .table("workout_bpm_summary")
        .awaitTermination()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the primary benefit to scheduling updates to gold tables as opposed to defining views is the ability to control costs associated with materializing results.
# MAGIC 
# MAGIC While returning results from this table will use some compute to scan the `workout_bpm_summary`, this design avoids having to scan and join files from multiple tables every time this view is queried.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workout_bpm_summary

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

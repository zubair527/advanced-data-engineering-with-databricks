# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Type 2 Slowly Changing Data
# MAGIC 
# MAGIC In this notebook, we'll create a silver table that contains the information we'll need to link workouts back to our heart rate recordings.
# MAGIC 
# MAGIC We'll use a Type 2 table to record this data, encoding the start and end times for each session. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_completed_workouts.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe how Slowly Changing Dimension tables can be implemented in the Lakehouse
# MAGIC - Use custom logic to implement a SCD Type 2 table with batch overwrite logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Set up path and checkpoint variables (these will be used later).

# COMMAND ----------

# MAGIC %run ../Includes/workouts-setup

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell is provided to allow for easy re-setting of this demo.
# MAGIC 
# MAGIC Even though we'll be overwriting our data in the target table each time we process records, we'll retain access to recent versions through Delta Lake's history. Completely dropping a table and deleting its data files will remove this history, and thus will generally not provide the desired production behavior.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS completed_workouts")

dbutils.fs.rm(Paths.completedWorkouts, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review `workouts_silver` Table
# MAGIC A helper function was defined to land and propagate a batch of data to the `workouts_silver` table.

# COMMAND ----------

process_silver_workouts()

# COMMAND ----------

# MAGIC %md
# MAGIC Load and preview the `workouts_silver` data.

# COMMAND ----------

workoutDF = spark.table("workouts_silver")
display(workoutDF)

# COMMAND ----------

# MAGIC %md
# MAGIC For this data, the `user_id` and `session_id` form a composite key. Each pair should eventually have 2 records present, marking the "start" and "stop" action for each workout.

# COMMAND ----------

display(workoutDF.groupby("user_id", "session_id").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Because we'll be triggering a shuffle in this notebook, we'll be explicit about how many partitions we want at the end of our shuffle.

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Completed Workouts Table
# MAGIC 
# MAGIC The query below matches our start and stop actions, capturing the time for each action. The `in_progress` field indicates whether or not a given workout session is ongoing.

# COMMAND ----------

def process_completed_workouts():
    spark.sql(f"""
        CREATE OR REPLACE TABLE completed_workouts 
        LOCATION '{Paths.completedWorkouts}'
        AS (
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
    
process_completed_workouts()

# COMMAND ----------

# MAGIC %md
# MAGIC You can now perform a query directly on your `completed_workouts` table to check your results. Uncomment the `WHERE` clauses below to confirm various functionality of the logic above.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM completed_workouts
# MAGIC -- WHERE in_progress=true                        -- where record is still awaiting end time
# MAGIC -- WHERE end_time IS NOT NULL                    -- where end time has been recorded
# MAGIC -- WHERE start_time IS NULL                      -- where end time arrived before start time
# MAGIC -- WHERE in_progress=true AND end_time IS NULL   -- confirm that no entries are valid with end_time

# COMMAND ----------

# MAGIC %md
# MAGIC Use the functions below to propagate another batch of records through the pipeline to this point.

# COMMAND ----------

process_silver_workouts()
process_completed_workouts()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM completed_workouts

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>

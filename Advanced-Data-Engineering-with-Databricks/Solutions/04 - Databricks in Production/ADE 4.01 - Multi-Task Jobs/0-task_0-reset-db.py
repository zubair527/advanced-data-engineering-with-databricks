# Databricks notebook source
import re

dbutils.widgets.text("mode", "pass")
mode = dbutils.widgets.get("mode")

username = spark.sql("SELECT current_user()").collect()[0][0]
database = f"""{re.sub("[^a-zA-Z0-9]", "_", username)}_dbacademy_jobs_db"""

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS task_0
# MAGIC (run_time TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS task_1
# MAGIC (task STRING, run_time TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS task_2
# MAGIC (task STRING, run_time TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO task_0 VALUES (current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM task_0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM task_0

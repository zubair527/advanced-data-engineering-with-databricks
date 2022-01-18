# Databricks notebook source
import re

dbutils.widgets.text("key", "value")
key = dbutils.widgets.get("key")

username = spark.sql("SELECT current_user()").collect()[0][0]
database = f"""{re.sub("[^a-zA-Z0-9]", "_", username)}_dbacademy_jobs_db"""
spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS task_3
# MAGIC (key STRING, run_time TIMESTAMP);

# COMMAND ----------

spark.sql(f"INSERT INTO task_3 VALUES ('{key}', current_timestamp())")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM task_3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM task_3

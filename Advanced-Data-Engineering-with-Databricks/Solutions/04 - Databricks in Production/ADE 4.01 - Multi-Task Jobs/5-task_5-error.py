# Databricks notebook source
import re

username = spark.sql("SELECT current_user()").collect()[0][0]
database = f"""{re.sub("[^a-zA-Z0-9]", "_", username)}_dbacademy_jobs_db"""
spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS task_5
# MAGIC (run_time TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO task_5 VALUES (current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM task_3 WHERE key="value"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(potato) FROM task_5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM task_4

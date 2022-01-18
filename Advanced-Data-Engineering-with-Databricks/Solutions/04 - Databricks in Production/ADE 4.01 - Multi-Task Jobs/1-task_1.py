# Databricks notebook source
import re

username = spark.sql("SELECT current_user()").collect()[0][0]
database = f"""{re.sub("[^a-zA-Z0-9]", "_", username)}_dbacademy_jobs_db"""
spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO task_1 VALUES ("task_1", current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO task_2 VALUES ("task_1", current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM task_1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM task_1

# Databricks notebook source
# MAGIC %run ./sql-setup $course="transactions" $mode="reset"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE new_records
# MAGIC (id INT, name STRING, value DOUBLE);
# MAGIC 
# MAGIC INSERT INTO new_records
# MAGIC VALUES (7, "Viktor", 7.4),
# MAGIC   (8, "Hiro", 8.2),
# MAGIC   (9, "Shana", 9.9);
# MAGIC 
# MAGIC OPTIMIZE new_records;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE updates
# MAGIC (id INT, name STRING, value DOUBLE, type STRING);
# MAGIC 
# MAGIC INSERT INTO updates
# MAGIC VALUES (2, "Omar", 15.2, "update"),
# MAGIC   (3, "", null, "delete"),
# MAGIC   (10, "Blue", 7.7, "insert"),
# MAGIC   (11, "Diya", 8.8, "update");

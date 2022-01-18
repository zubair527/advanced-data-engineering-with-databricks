# Databricks notebook source
# MAGIC %run ./ade-setup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gym_mac_logs")
dbutils.fs.rm(Paths.gymMacLogs, True)
spark.read.json(f"{URI}/gym-logs/").write.option("path", Paths.gymMacLogs).saveAsTable("gym_mac_logs")


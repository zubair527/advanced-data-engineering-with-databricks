# Databricks notebook source
URI = "wasbs://courseware@dbacademy.blob.core.windows.net/structured-streaming-lakehouse/v01"

# COMMAND ----------

# MAGIC %run ./_user $lesson="cdc"

# COMMAND ----------

import pyspark.sql.functions as F

# Moved to _user
# dbutils.widgets.text("course", "cdc")
# course = dbutils.widgets.get("course")
# username = spark.sql("SELECT current_user()").collect()[0][0]
# userhome = f"dbfs:/user/{username}/{course}"
# database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""
# print(f"""
# username: {username}
# userhome: {userhome}
# database: {database}""")

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

# COMMAND ----------

filepath = f"{userhome}/cdc_raw"

spark.sql(f"""
  CREATE TABLE cdc_raw
  DEEP CLONE delta.`{URI}/pii/raw`
  LOCATION '{filepath}'
""")

# COMMAND ----------

class FileArrival:
    def __init__(self, demohome, reset=True, max_batch=3):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.userdir = demohome+ "/raw"
        self.batch = 1
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.userdir, True)
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= max_batch:
                (self.rawDF.filter(F.col("batch") == self.batch)
                    .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                    .write
                    .mode("append")
                    .format("json")
                    .save(self.userdir))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("batch") == self.batch)
                .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                .write
                .mode("append")
                .format("json")
                .save(self.userdir))
            self.batch += 1

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

Raw = FileArrival(userhome)


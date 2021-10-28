# Databricks notebook source
# MAGIC %run ./_user $lesson="clones"

# COMMAND ----------

import pyspark.sql.functions as F
import re

# Moved to _user
# course = "clones"
# username = spark.sql("SELECT current_user()").collect()[0][0]
# userhome = f"dbfs:/user/{username}/{course}"
# database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""
# print(f"""
# username: {username}
# userhome: {userhome}
# database: {database}""")

spark.sql(f"SET c.userhome = {userhome}")

dbutils.widgets.text("mode", "cleanup")
mode = dbutils.widgets.get("mode")

spark.conf.set("spark.databricks.io.cache.enabled", "false")

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}");

# COMMAND ----------

import time
import numpy as np
import pandas as pd

class DataPrep:
    def __init__(self):
        self.deviceTypes = ["A", "B", "C", "D"]
        self.fileID = 0
    
    def prep(self, numFiles=3, numRows=100):
        for i in range(numFiles):
            self.make_data()
            
    def make_data(self, numRows=1000):
        startTime=int(time.time()*1000)
        timestamp = startTime + (self.fileID * 60000) + np.random.randint(-10000, 10000, size=numRows)
        deviceId = np.random.randint(0, 100, size=numRows)
        deviceType = np.random.choice(self.deviceTypes, size=numRows)
        signalStrength = np.random.random(size=numRows)
        data = [timestamp, deviceId, deviceType, signalStrength]
        columns = ["time", "device_id", "sensor_type", "signal_strength"]
        tempDF = spark.createDataFrame(pd.DataFrame(data=zip(*data), columns = columns))
        tempDF.write.format("delta").mode("append").saveAsTable("sensors_prod")
        self.fileID+=1

# COMMAND ----------

if mode != "cleanup":
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS sensors_prod
        (time LONG COMMENT 'event timestamp in ms since epoch', 
        device_id LONG COMMENT 'device IDs, integer only', 
        sensor_type STRING COMMENT 'sensor type identifier; single upper case letter', 
        signal_strength DOUBLE COMMENT 'decimal value between 0 and 1')
        USING DELTA
        LOCATION '{userhome}/prod/sensors'
    """);
    Prepper = DataPrep()
    Prepper.prep()

# COMMAND ----------

def check_files(table_name):
    filepath = spark.sql(f"DESCRIBE EXTENDED {table_name}").filter("col_name == 'Location'").select("data_type").collect()[0][0]
    filelist = dbutils.fs.ls(filepath)
    filecount = len([file for file in filelist if file.name != "_delta_log/" ])
    print(f"Count of all data files in {table_name}: {filecount}\n")
    return filelist

# COMMAND ----------

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)


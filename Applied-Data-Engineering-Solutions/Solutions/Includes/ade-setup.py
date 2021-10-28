# Databricks notebook source
# MAGIC %run ./data-source

# COMMAND ----------

import pyspark
import pyspark.sql.functions as F
import re

username = spark.sql("SELECT current_user()").first()[0]
userhome = f"dbfs:/user/{username}"
database = f"""ade_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

# COMMAND ----------

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

# COMMAND ----------

class BuildPaths:
  
    def __init__(self, base_path=f"{userhome}/ade/delta_tables", db=database):
        spark.sql(f"USE {db}")
        
        self.basePath = base_path
        self.raw = f"{userhome}/raw"
        self.producer = f"{self.raw}/producer"
        self.producer30m = f"{self.raw}/producer30m" 
        self.usersProducer = f"{self.raw}/users_producer"
        self.source = f"{userhome}/source"
        self.sourceDaily = f"{self.source}/source_daily"
        self.source30m = f"{self.source}/source_30m"
        self.checkpointPath = f"{self.basePath}/_checkpoints"    
        
        self.bronzeTable = f"{self.basePath}/bronze"
        self.bronzeCheckpoint = f"{self.checkpointPath}/bronze_checkpoint"
        self.bronzeDevTable = f"{self.basePath}/bronze_dev"
        self.bronzeDevCheckpoint = f"{self.checkpointPath}/bronze_dev_checkpoint"

        self.silverPath = f"{self.basePath}/silver"
        self.silverCheckpointPath = f"{self.checkpointPath}/silver"

        self.silverRecordingsTable = f"{self.silverPath}/recordings"
        self.silverWorkoutsTable = f"{self.silverPath}/workouts"
        self.completedWorkouts = f"{self.silverPath}/completed_workouts"
        self.workoutBpm = f"{self.silverPath}/workout_bpm"
        self.gymMacLogs = f"{self.silverPath}/gym_mac_logs"

        self.silverRecordingsCheckpoint = f"{self.silverCheckpointPath}/silver_recordings"
        self.silverWorkoutsCheckpoint = f"{self.silverCheckpointPath}/workouts"
        self.completedWorkoutsCheckpoint = f"{self.silverCheckpointPath}/completed_workouts"
        self.workoutBpmCheckpoint = f"{self.silverCheckpointPath}/workout_bpm"
        self.gymMacLogsCheckpoint = f"{self.silverCheckpointPath}/gym_mac_logs"
        
        self.piiPath = f"{self.basePath}/pii"
        self.piiCheckpointPath = f"{self.checkpointPath}/pii"
        self.rawUserReg = f"{self.piiPath}/raw_user_reg"
        self.registeredUsers = f"{self.piiPath}/registered_users"
        self.registeredUsersCheckpointPath = f"{self.piiCheckpointPath}/registered_users"
        self.userLookup = f"{self.piiPath}/user_lookup"
        self.userLookupCheckpointPath = f"{self.piiCheckpointPath}/user_lookup"
        self.users = f"{self.piiPath}/users"
        self.usersCheckpointPath = f"{self.piiCheckpointPath}/users"
        self.userBins = f"{self.piiPath}/user_bins"
        self.deleteRequests = f"{self.piiPath}/delete_requests"
        
        self.goldPath = f"{self.basePath}/gold"
        self.goldCheckpointPath = f"{self.checkpointPath}/gold"
        
        self.workoutBpmSummary = f"{self.goldPath}/workout_bpm_summary"         
        self.errorRateTable = f"{self.goldPath}/error_rate"
        
        self.workoutBpmSummaryCheckpoint = f"{self.goldCheckpointPath}/workout_bpm_summary"
        
        self.dateLookup = f"{self.basePath}/date_lookup"
        
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n")

# COMMAND ----------

Paths = BuildPaths()

# COMMAND ----------

if mode == "reset":
    dbutils.fs.rm(Paths.basePath, True)

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS date_lookup
  DEEP CLONE delta.`{URI}/date-lookup`
  LOCATION '{Paths.dateLookup}'
""")

# COMMAND ----------

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS source_{database} CASCADE")
    dbutils.fs.rm(Paths.basePath, True)
    dbutils.fs.rm(Paths.source, True)
    dbutils.fs.rm(Paths.raw, True)


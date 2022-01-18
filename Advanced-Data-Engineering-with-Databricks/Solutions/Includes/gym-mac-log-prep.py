# Databricks notebook source
# MAGIC %run ./ade-setup $mode="reset"

# COMMAND ----------

source = f"{URI}/gym-logs/"
gym_mac_logs = userhome + "/gym/mac_logs/"
files = dbutils.fs.ls(source)

dbutils.fs.rm(gym_mac_logs, True)

for curr_file in [file.name for file in files if file.name.startswith(f"2019120")]:
    dbutils.fs.cp(source + curr_file, gym_mac_logs + curr_file)

# COMMAND ----------

class FileArrival:
    def __init__(self, source, userdir):
        self.source = source
        self.userdir = userdir
        self.curr_day = 10
    
    def arrival(self, continuous=False):
        files = dbutils.fs.ls(self.source)
        if self.curr_day > 16:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_day <= 16:
                for curr_file in [file.name for file in files if file.name.startswith(f"201912{self.curr_day}")]:
                    dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
                self.curr_day += 1
        else:
            for curr_file in [file.name for file in files if file.name.startswith(f"201912{self.curr_day}")]:
                dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
            self.curr_day += 1

# COMMAND ----------

NewFile = FileArrival(source, gym_mac_logs)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gym_mac_logs")
dbutils.fs.rm(Paths.gymMacLogs, True)
dbutils.fs.rm(Paths.gymMacLogsCheckpoint, True)


# Databricks notebook source
# MAGIC %run ./ade-setup

# COMMAND ----------

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

producerDF = spark.table(f"source_{database}.producer").withColumn("day", F.when(F.col("date") <= '2019-12-01', 1).otherwise(F.dayofmonth("date")))

# COMMAND ----------

class FileArrival:
    def __init__(self, demohome, reset=True, starting_batch=1, max_batch=16):
        self.rawDF = spark.table(f"source_{database}.producer").withColumn("day", F.when(F.col("date") <= '2019-12-01', 1).otherwise(F.dayofmonth("date")))
        self.userdir = demohome
        self.max_batch = max_batch
        if reset == True:
            self.batch = starting_batch
            dbutils.fs.rm(self.userdir, True)
        else:
            self.batch = spark.read.json(Paths.sourceDaily).select(F.max(F.when((F.col("timestamp")/1000).cast("timestamp").cast("date") <= '2019-12-01', 1).otherwise(F.dayofmonth((F.col("timestamp")/1000).cast("timestamp").cast("date"))))).collect()[0][0]
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= self.max_batch:
                (self.rawDF.filter(F.col("day") == self.batch).drop("date", "week_part", "day")
                    .write
                    .mode("append")
                    .format("json")
                    .save(self.userdir))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("day") == self.batch).drop("date", "week_part", "day")
                .write
                .mode("append")
                .format("json")
                .save(self.userdir))
            self.batch += 1

# COMMAND ----------

if mode == "reset":
    dbutils.fs.rm(Paths.sourceDaily, True)
    Raw = FileArrival(Paths.sourceDaily)
else:
    Raw = FileArrival(Paths.sourceDaily, reset=False)

# COMMAND ----------

Raw.arrival()


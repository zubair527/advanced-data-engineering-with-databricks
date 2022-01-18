# Databricks notebook source
# MAGIC %run "./ade-setup" $mode="reset"

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS source_{database} CASCADE")

dbutils.fs.rm(Paths.rawUserReg, True)
dbutils.fs.rm(Paths.usersProducer, True)

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS bronze
  DEEP CLONE delta.`{URI}/bronze`
  LOCATION '{Paths.bronzeTable}'
""")

# COMMAND ----------

usersProducerSource = f"{userhome}/users_producer"

dbutils.fs.rm(usersProducerSource, True)

(spark.read
 .format("json")
 .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
 .load(f"{URI}/user-reg")
 .select("*", F.col("registration_timestamp").cast("timestamp").cast("date").alias("date"))
 .createOrReplaceTempView("temp_table"))

spark.sql("""
    CREATE OR REPLACE TEMP VIEW final_df AS
    SELECT device_id, mac_address, registration_timestamp, user_id, CASE
      WHEN date < "2019-12-01"
        THEN 0
      ELSE dayofmonth(date)
      END AS batch
    FROM temp_table
""")

spark.table("final_df").write.save(usersProducerSource)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS source_{database}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS source_{database}.users_producer
  DEEP CLONE delta.`{usersProducerSource}`
  LOCATION '{Paths.usersProducer}'
""")

# COMMAND ----------

class FileArrival:
    def __init__(self, demohome, tablename, reset=True, starting_batch=0, max_batch=15):
        self.rawDF = spark.read.format("delta").table(tablename)
        self.userdir = demohome
        self.batch = starting_batch
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.userdir, True)
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= self.max_batch:
                (self.rawDF.filter(F.col("batch") == self.batch).drop("batch")
                    .write
                    .mode("append")
                    .format("json")
                    .save(self.userdir))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("batch") == self.batch).drop("batch")
                .write
                .mode("append")
                .format("json")
                .save(self.userdir))
            self.batch += 1

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

Raw = FileArrival(Paths.rawUserReg, f"source_{database}.users_producer")

# COMMAND ----------

Raw.arrival()



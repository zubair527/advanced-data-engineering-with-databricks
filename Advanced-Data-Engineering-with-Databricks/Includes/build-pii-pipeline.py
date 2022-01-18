# Databricks notebook source
# MAGIC %run ./ade-setup

# COMMAND ----------

if mode == "reset":
    spark.sql("DROP TABLE IF EXISTS users")
    dbutils.fs.rm(Paths.users, True)
    dbutils.fs.rm(Paths.usersCheckpointPath, True)

    spark.sql("DROP TABLE IF EXISTS delete_requests")
    dbutils.fs.rm(Paths.deleteRequests, True)

    spark.sql(f"""
      CREATE TABLE users
      (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP)
      USING DELTA
      LOCATION '{Paths.users}'
    """)

    spark.sql("DROP TABLE IF EXISTS user_lookup")
    dbutils.fs.rm(Paths.userLookup, True)

    spark.sql("DROP TABLE IF EXISTS user_bins")
    dbutils.fs.rm(Paths.userBins, True)

# COMMAND ----------

def create_user_lookup():
    (spark.read
        .format("json")
        .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
        .load(f"{URI}/user-reg")
        .selectExpr(f"sha2(concat(user_id,'BEANS'), 256) AS alt_id", "device_id", "mac_address", "user_id")
        .write
        .option("path", Paths.userLookup)
        .mode("overwrite")
        .saveAsTable("user_lookup"))

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())

def batch_rank_upsert(microBatchDF, batchId):
    appId = "batch_rank_upsert"
    
    (microBatchDF
        .filter(F.col("update_type").isin(["new", "update"]))
        .withColumn("rank", F.rank().over(window)).filter("rank == 1").drop("rank")
        .createOrReplaceTempView("ranked_updates"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING ranked_updates r
        ON u.alt_id=r.alt_id
        WHEN MATCHED AND u.updated < r.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

    (microBatchDF
         .filter("update_type = 'delete'")
         .select(
            "alt_id", 
            F.col("updated").alias("requested"), 
            F.date_add("updated", 30).alias("deadline"), 
            F.lit("requested").alias("status"))
        .write
        .format("delta")
        .mode("append")
        .option("txnVersion", batchId)
        .option("txnAppId", appId)
        .option("path", Paths.deleteRequests)
        .saveAsTable("delete_requests"))

def process_users():
    schema = """
        user_id LONG, 
        update_type STRING, 
        timestamp FLOAT, 
        dob STRING, 
        sex STRING, 
        gender STRING, 
        first_name STRING, 
        last_name STRING, 
        address STRUCT<
            street_address: STRING, 
            city: STRING, 
            state: STRING, 
            zip: INT
    >"""
    
    (spark.readStream
        .table("bronze")
        .filter("topic = 'user_info'")
        .dropDuplicates()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
        .select(F.sha2(F.concat(F.col("user_id"), F.lit("BEANS")), 256).alias("alt_id"),
            F.col('timestamp').cast("timestamp").alias("updated"),
            F.to_date('dob','MM/dd/yyyy').alias('dob'),
            'sex', 'gender','first_name','last_name',
            'address.*', "update_type")
        .writeStream
        .foreachBatch(batch_rank_upsert)
        .outputMode("update")
        .option("checkpointLocation", Paths.usersCheckpointPath)
        .trigger(once=True)
        .start()
        .awaitTermination())

# COMMAND ----------

def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
    return (F.when((age_col < 18), "under 18")
            .when((age_col >= 18) & (age_col < 25), "18-25")
            .when((age_col >= 25) & (age_col < 35), "25-35")
            .when((age_col >= 35) & (age_col < 45), "35-45")
            .when((age_col >= 45) & (age_col < 55), "45-55")
            .when((age_col >= 55) & (age_col < 65), "55-65")
            .when((age_col >= 65) & (age_col < 75), "65-75")
            .when((age_col >= 75) & (age_col < 85), "75-85")
            .when((age_col >= 85) & (age_col < 95), "85-95")
            .when((age_col >= 95), "95+")
            .otherwise("invalid age").alias("age"))

def build_user_bins():
    (spark.table("users")
         .join(
            spark.table("user_lookup")
                .select("alt_id", "user_id"), 
            ["alt_id"], 
            "left")
        .select("user_id", 
                age_bins(F.col("dob")),
                "gender", 
                "city", 
                "state")
         .write
        .format("delta")
        .option("path", Paths.userBins)
        .mode("overwrite")
        .saveAsTable("user_bins"))

# COMMAND ----------

create_user_lookup()
process_users()
build_user_bins()


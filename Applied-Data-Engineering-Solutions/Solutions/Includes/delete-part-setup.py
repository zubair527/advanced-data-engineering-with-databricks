# Databricks notebook source
# MAGIC %run ./reset-&-install-datasets

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE users
  (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP)
  USING DELTA
  LOCATION '{Paths.users}'
""")

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
    
    salt = "BEANS"
  
    (spark.readStream
        .table("bronze")
        .filter("topic = 'user_info'")
        .dropDuplicates()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
        .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
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
process_users()


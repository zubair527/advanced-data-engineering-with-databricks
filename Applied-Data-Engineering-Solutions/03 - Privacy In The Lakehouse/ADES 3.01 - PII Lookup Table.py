# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a Pseudonymized PII Lookup Table
# MAGIC 
# MAGIC In this lesson we'll create a pseudonymized key for storing potentially sensitive user data.
# MAGIC 
# MAGIC Our approach in this notebook is fairly straightforward; some industries may require more elaborate de-identification to guarantee privacy.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_user_lookup.png" width="60%" />
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe the purpose of "salting" before hashing
# MAGIC - Apply salted hashing to sensitive data for pseudonymization
# MAGIC - Combine Auto Loader and Change Data Feed to allow incremental inserts and propagation of deleted data

# COMMAND ----------

# MAGIC %md
# MAGIC Begin by running the following cell to set up relevant databases and paths.

# COMMAND ----------

# MAGIC %run "../Includes/users-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Load Bronze Data
# MAGIC 
# MAGIC The following cell defines and executes logic to incrementally ingest data into the `registered_users` table with Auto Loader.

# COMMAND ----------

def ingest_user_reg():
    (spark.readStream
        .schema("device_id LONG, mac_address STRING, registration_timestamp DOUBLE, user_id LONG")
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(Paths.rawUserReg)
        .writeStream
        .option("checkpointLocation", Paths.registeredUsersCheckpointPath)
        .trigger(once=True)
        .option("path", Paths.registeredUsers)
        .table("registered_users")
        .awaitTermination())
    
ingest_user_reg()
display(spark.table("registered_users"))

# COMMAND ----------

# MAGIC %md
# MAGIC Another custom class was initialized in the setup script to land a new batch of data in our source directory. 
# MAGIC 
# MAGIC Execute the following cell and note the difference in the total number of rows in our tables.

# COMMAND ----------

Raw.arrival()
ingest_user_reg()
display(spark.table("registered_users"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a Salt to Natural Key
# MAGIC We'll start by defining a salt, here in plain text. We'll combine this salt with our natural key, `user_id`, before applying a hash function to generate a pseudonymized key.
# MAGIC 
# MAGIC For greater security, we could upload the salt as a secret using the Databricks <a href="https://docs.databricks.com/security/secrets/secrets.html" target="_blank">Secrets</a> utility.

# COMMAND ----------

salt = 'BEANS'

# COMMAND ----------

# MAGIC %md
# MAGIC Preview what your new key will look like.

# COMMAND ----------

display(spark.sql(f"""
SELECT *, sha2(concat(user_id,"{salt}"), 256)
FROM registered_users
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that it is theoretically possible to link the original key and pseuo-ID if the hash function and the salt are known.
# MAGIC 
# MAGIC Here, we use this method to add a layer of obfuscation; in production, you may wish to have a much more sophisticated hashing method.
# MAGIC 
# MAGIC **NOTE**: The table is being created with Change Data Feed enabled, as this will be used later in the pipeline.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS user_lookup
(alt_id string, device_id long, mac_address string, user_id long)
USING DELTA 
LOCATION '{Paths.userLookup}'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Here we're just creating our `user_lookup` table. In the next notebook, we'll use this pseudo-ID as the sole link to user PII.
# MAGIC 
# MAGIC By controlling access to the link between our `alt_id` and other natural keys, we'll be able to prevent linking PII to other user data throughout our system.

# COMMAND ----------

def load_user_lookup():
    (spark.readStream
        .table("registered_users")
        .selectExpr(f"sha2(concat(user_id,'{salt}'), 256) AS alt_id", "device_id", "mac_address", "user_id")
        .writeStream
        .option("checkpointLocation", Paths.userLookupCheckpointPath)
        .trigger(once=True)
        .table("user_lookup")
        .awaitTermination())
    
load_user_lookup()
display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC Process another batch of data below to confirm that the incremental processing is working through the entire pipeline.

# COMMAND ----------

Raw.arrival()
ingest_user_reg()
load_user_lookup()
display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC The code below ingests all the remaining records to put 100 total users in the `user_lookup` table.

# COMMAND ----------

Raw.arrival(continuous=True)
ingest_user_reg()
load_user_lookup()
display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll apply this same hashing method to process and store PII data in the next lesson.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Setting Up Tables
# MAGIC Managing database and table metadata, locations, and configurations at the beginning of project can help to increase data security, discoverability, and performance.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, students will be able to:
# MAGIC - Set database locations
# MAGIC - Specify database comments
# MAGIC - Set table locations
# MAGIC - Specify table comments
# MAGIC - Specify column comments
# MAGIC - Use table properties for custom tagging
# MAGIC - Explore table metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Variables
# MAGIC 
# MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

# COMMAND ----------

# MAGIC %run ../Includes/sql-setup $lesson="demo" $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Hive Variables
# MAGIC 
# MAGIC While not a pattern that is generally recommended in Spark SQL, this notebook will use some Hive variables to substitute in string values derived from the account email of the current user.
# MAGIC 
# MAGIC The following cell demonstrates this pattern.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "${c.database}";

# COMMAND ----------

# MAGIC %md
# MAGIC Using this syntax is identical to typing the associated string value into a SQL query.
# MAGIC 
# MAGIC Run the following to make sure no database with the provided name exists.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS ${c.database} CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Database with Options
# MAGIC 
# MAGIC The following cell demonstrates the syntax for creating a database while:
# MAGIC 1. Setting a database comment
# MAGIC 1. Specifying a database location
# MAGIC 1. Adding an arbitrary key-value pair as a database property
# MAGIC 
# MAGIC An arbitrary directory on the DBFS root is being used for the location; in any stage of development or production, it is best practice to create databases in secure cloud object storage with credentials locked down to appropriate teams within the organization.
# MAGIC 
# MAGIC **NOTE**: Remember that by default, all managed tables will be created within the directory declared as the location when a database is created.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE ${c.database}
# MAGIC COMMENT "This is a test database"
# MAGIC LOCATION "${c.userhome}"
# MAGIC WITH DBPROPERTIES (contains_pii = true)

# COMMAND ----------

# MAGIC %md
# MAGIC All of the comments and properties set during database declaration can be reviewed using `DESCRIBE DATABASE EXTENDED`.
# MAGIC 
# MAGIC This information can aid in data discovery, auditing, and governance. Having proactive rules about how databases will be created and tagged can help prevent accidental data exfiltration, redundancies, and deletions.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED ${c.database}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Table with Options
# MAGIC The following cell demonstrates creating a **managed** Delta Lake table while:
# MAGIC 1. Setting a column comment
# MAGIC 1. Setting a table comment
# MAGIC 1. Adding an arbitrary key-value pair as a table property
# MAGIC 
# MAGIC **NOTE**: A number of Delta Lake configurations are also set using `TBLPROPERTIES`. When using this field as part of an organizational approach to data discovery and auditting, users should be made aware of which keys are leveraged for modifying default Delta Lake behaviors.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${c.database}.pii_test
# MAGIC (id INT, name STRING COMMENT "PII")
# MAGIC COMMENT "Contains PII"
# MAGIC TBLPROPERTIES ('contains_pii' = True) 

# COMMAND ----------

# MAGIC %md
# MAGIC Much like the command for reviewing database metadata settings, `DESCRIBE EXTENDED` allows users to see all of the comments and properties for a given table.
# MAGIC 
# MAGIC **NOTE**: Delta Lake automatically adds several table properties on table creation.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED ${c.database}.pii_test

# COMMAND ----------

# MAGIC %md
# MAGIC Below the code from above is replicated with the addition of specifying a location, creating an **external** table.
# MAGIC 
# MAGIC **NOTE**: The only thing that differentiates managed and external tables is this location setting. Performance of managed and external tables should be equivalent with regards to latency, but the results of SQL DDL statements on these tables differ drastically.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${c.database}.pii_test_2
# MAGIC (id INT, name STRING COMMENT "PII")
# MAGIC COMMENT "Contains PII"
# MAGIC LOCATION "${c.userhome}/pii_test_2"
# MAGIC TBLPROPERTIES ('contains_pii' = True) 

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, the only differences in the extended description of the table have to do with the table location and type.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED ${c.database}.pii_test_2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Table Metadata
# MAGIC 
# MAGIC Assuming that rules are followed appropriately when creating databases and tables, comments, table properties, and other metadata can be interacted with programmatically for discovering datasets for governance and auditing purposes.
# MAGIC 
# MAGIC The Python code below demonstrates parsing the table properties field, filtering those options that are specifically geared toward controlling Delta Lake behavior. In this case, logic could be written to further parse these properties to identify all tables in a database that contain PII.

# COMMAND ----------

def parse_table_keys(database):
    table_keys = {}
    for table in spark.sql(f"SHOW TABLES IN {database}").collect():
        table_name = table[1]
        key_values = spark.sql(f"DESCRIBE EXTENDED {database}.{table_name}").filter("col_name = 'Table Properties'").collect()[0][1][1:-1].split(",")
        table_keys[table_name] = [kv for kv in key_values if not kv.startswith("delta.")]
    return table_keys

parse_table_keys(database)   

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

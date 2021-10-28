# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimizing Data Storage with Delta Lake
# MAGIC 
# MAGIC Databricks supports a number of optimization for clustering data and improving directory and file skipping while scanning and loading data files. While some of these optimizations will use the word "index" in describing the process used, these indices differ from the algorithms many users will be familiar with from traditional SQL database systems.
# MAGIC 
# MAGIC In this notebook we'll explore how optional data storage and optimization settings on Delta Lake interact with file size and data skipping.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, students will be able to:
# MAGIC - Describe default behavior for statistics collection and file skipping on Delta Lake
# MAGIC - Identify columns well-suited to partitioning
# MAGIC - Use `OPTIMIZE` to compact small files
# MAGIC - Apply Z-order to optimize file skipping on high cardinality fields
# MAGIC - Use bloom filters to speed up queries on fields with arbitrary text

# COMMAND ----------

# MAGIC %run ../Includes/data-storage-setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Delta Table

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE no_part_table
    LOCATION '{userhome}/no_part_table'
    AS SELECT * FROM raw_data
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake File Statistics
# MAGIC 
# MAGIC By default, Delta Lake will capture statistics on the first 32 columns that appear in a table. These statistics indicate the total number of records per file, as well as the minimum, maximum, and null value counts for each of the columns.
# MAGIC 
# MAGIC Statistics are recorded in the Delta Lake transaction log files. Files are initially committed in the JSON format, but are compacted to Parquet format automatically to accelerate metadata retrieval.
# MAGIC 
# MAGIC Transaction logs can be viewed in the `_delta_log` directory within the table location.

# COMMAND ----------

dbutils.fs.ls(f"{userhome}/no_part_table/_delta_log")

# COMMAND ----------

# MAGIC %md
# MAGIC JSON log files can be easily parsed with Spark. Statistics for each file are accessible in the `add` column.
# MAGIC 
# MAGIC When a query with a selective filter (`WHERE` clause) is executed against a Delta Lake table, the query optimizer uses the information stored in the transaction logs to identify files that **may** contain records matching the conditional filter.

# COMMAND ----------

display(spark.read.json(f"{userhome}/no_part_table/_delta_log/00000000000000000000.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that columns used when creating Z-order or Bloom Filter indexes need to have statistics collected. Even without additional optimization metrics, statistics will always be leveraged for file skipping.
# MAGIC 
# MAGIC **NOTE**: Calculating statistics on free-form text fields (product reviews, user messages, etc.) can be time consuming. For best performance, set these fields later in the schema and [change the number of columns that statistics are collected on](https://docs.databricks.com/delta/optimizations/file-mgmt.html#data-skipping).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning Delta Lake Tables
# MAGIC 
# MAGIC The partitioning method used in Delta Lake is similar to that used by Hive or Spark with Parquet (recall that Delta Lake data files are stored as Parquet).
# MAGIC 
# MAGIC When a column is used to partition a table, each unique value found in that column will create a separate directory for data. When choosing partition columns, it's good to consider the following:
# MAGIC 1. How many total values will be present in a column?
# MAGIC 1. How many total records will share a given value for a column?
# MAGIC 1. Will records with a given value continue to arrive indefinitely?
# MAGIC 
# MAGIC **NOTE**: When in doubt, do not partition data at all. Other data skipping features in Delta Lake can achieve similar speeds as partitioning, but data that is over-partitioned or incorrectly partitioned will suffer greatly (and require a full rewrite of all data files to remedy).
# MAGIC 
# MAGIC Columns representing measures of time and low-cardinality fields used frequently in queries are good candidates for partitioning. The code below creates a table partitioned by date using [generated columns](https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns).

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE date_part_table (
      key STRING,
      value BINARY,
      topic STRING,
      partition LONG,
      offset LONG,
      timestamp LONG,
      p_date DATE GENERATED ALWAYS AS (CAST(CAST(timestamp/1000 AS timestamp) AS DATE))
    )
    PARTITIONED BY (p_date)
    LOCATION '{userhome}/date_part_table'
""")

spark.table("raw_data").write.mode("append").saveAsTable("date_part_table")

# COMMAND ----------

# MAGIC %md
# MAGIC Listing the location used for the table reveals that the unique values in the partition column are used to generate data directories. Note that the Parquet format used to store the data for Delta Lake leverages these partitions directly when determining column value (the column values for `p_date` are not stored redundantly within the data files).

# COMMAND ----------

dbutils.fs.ls(f"{userhome}/date_part_table")

# COMMAND ----------

# MAGIC %md
# MAGIC The data in this table look largely the same, except that more files were written because of the separation of data into separate directories based on the date.

# COMMAND ----------

display(spark.read.json(f"{userhome}/date_part_table/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC When running a query that filters data on a column used for partitioning, partitions not matching a conditional statement will be skipped entirely. Delta Lake also have several operations (including `OPTIMIZE` commands) that can be applied at the partition level.
# MAGIC 
# MAGIC Note that because data files will be separated into different directories based on partition values, files cannot be combined or compacted across these partition boundaries. Depending on the size of data in a given table, the "right size" for a partition will vary, but if most partitions in a table will not contain at least 1GB of data, the table is likely over-partitioned, which will lead to slowdowns for most general queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p_date, COUNT(*) FROM date_part_table GROUP BY p_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Compaction
# MAGIC Delta Lake supports the `OPTIMIZE` operation, which performs file compaction. The [target file size can be auto-tuned](https://docs.databricks.com/delta/optimizations/file-mgmt.html#autotune-based-on-table-size) by Databricks, and is typically between 256 MB and 1 GB depending on overall table size.
# MAGIC 
# MAGIC Note that data files cannot be combined across partitions. As such, some tables will benefits from not using partitions to minimize storage costs and total number of files to scan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-Order Indexing
# MAGIC 
# MAGIC Z-order indexing is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC Don't worry about <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">the math</a> (tl;dr: Z-order maps multidimensional data to one dimension while preserving locality of the data points).
# MAGIC 
# MAGIC Multiple columns can be used for Z-ordering, but the algorithm does lose some efficiency with each additional column. The best columns for Z-order are high cardinality columns that will be used commonly in queries.
# MAGIC 
# MAGIC Z-order must be executed at the same time as `OPTIMIZE`, as it requires rewriting data files.
# MAGIC 
# MAGIC Below is the code to Z-order and optimize the `date_part_table` by `timestamp` (this might be useful for regular queries within granular time ranges).

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE date_part_table
# MAGIC ZORDER BY (timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the metrics will provide an overview of what happened during the operation; reviewing the table history will also provide this information.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY date_part_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bloom Filter Indexes
# MAGIC 
# MAGIC While Z-order provides useful data clustering for high cardinality data, it's often most effective when working with queries that filter against continuous numeric variables.
# MAGIC 
# MAGIC Bloom filters provide an efficient algorithm for probabilistically identifying files that may contain data using fields containing arbitrary text. Appropriate fields would include hashed values, alphanumeric codes, or free-form text fields.
# MAGIC 
# MAGIC Bloom filters calculate indexes that indicate the likelihood a given value **could** be in a file; the size of the calculated index will vary based on the number of unique values present in the field being indexed and the configured tolerance for false positives.
# MAGIC 
# MAGIC **NOTE**: A false positive would be a file that the index thinks could have a matching record but does not. Files containing data matching a selective filter will never be skipped; false positives just mean that extra time was spent scanning files without matching records.
# MAGIC 
# MAGIC Looking at the distribution for the `key` field, this is an ideal candidate for this technique.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT key, count(*) FROM no_part_table GROUP BY key ORDER BY count(*) ASC

# COMMAND ----------

# MAGIC %md
# MAGIC The code below sets a bloom filter index on the `key` field with a false positivity allowance of 0.1%.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE BLOOMFILTER INDEX
# MAGIC ON TABLE date_part_table
# MAGIC FOR COLUMNS(key OPTIONS (fpp=0.1, numItems=200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hands-On
# MAGIC 
# MAGIC Go through the process of adding a Z-order index and bloom filter index to the `no_part_table`. Review the history for the table to confirm the operations were successful.
# MAGIC 
# MAGIC If you have extra time, also create a 3rd table partitioned by `topic` and `date`, and then apply the same Z-order and bloom filter indices.

# COMMAND ----------

# TODO
<FILL-IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

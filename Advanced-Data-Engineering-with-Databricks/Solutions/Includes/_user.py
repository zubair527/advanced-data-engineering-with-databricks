# Databricks notebook source
import re

course = "ades"

dbutils.widgets.text("lesson", "unknown")
lesson = dbutils.widgets.get("lesson").lower()
assert lesson != "unknown", "The lesson was not specified to /Includes/_user."

username = spark.sql("SELECT current_user()").collect()[0][0]
username_clean = re.sub("[^a-zA-Z0-9]", "_", username)

userhome_prefix = f"dbfs:/user/{username}/dbacademy/{course}"
userhome = f"{userhome_prefix}/{lesson}"

database_prefix = f"""{username_clean}_dbacademy_{course}"""
database = f"{database_prefix}_{lesson}"


# COMMAND ----------

print(f"username: {username}")
print(f"userhome: {userhome}")
print(f"database: {database}")



# Databricks notebook source
# MAGIC %md
# MAGIC # Data Prep Notebook

# COMMAND ----------

from pyspark.sql import functions as F

# Reading data repeatedly
df = spark.read.table("raw_sales")
df_filtered = df.filter(F.col("amount") > 100)
df_filtered.display()

# COMMAND ----------

# Re-reading same table later without cache
df_again = spark.read.table("raw_sales")
df_grouped = df_again.groupBy("category").count()
df_grouped.display()

# COMMAND ----------

# Inline SQL with SELECT *
spark.sql("SELECT * FROM raw_sales WHERE region = 'US'").display()

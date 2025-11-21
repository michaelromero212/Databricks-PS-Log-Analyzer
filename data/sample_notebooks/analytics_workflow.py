# Databricks notebook source
# Analytics Workflow

# COMMAND ----------

# Join small table to large table without broadcast
large_df = spark.read.table("fact_transactions")
small_df = spark.read.table("dim_currency")

joined_df = large_df.join(small_df, "currency_code")
joined_df.write.mode("overwrite").saveAsTable("processed_transactions")

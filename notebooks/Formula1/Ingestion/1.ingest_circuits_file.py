# Databricks notebook source
# MAGIC %md Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("/mnt/victormrlucasformula1dl/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------


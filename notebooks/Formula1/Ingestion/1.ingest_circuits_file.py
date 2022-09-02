# Databricks notebook source
# MAGIC %md Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),  
                                     StructField("name", StringType(), True), 
                                     StructField("location", StringType(), True), 
                                     StructField("country", StringType(), True), 
                                     StructField("lat", DoubleType(), True), 
                                     StructField("lng", DoubleType(), True), 
                                     StructField("alt", IntegerType(), True), 
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read\
               .option("header",True)\
               .schema(circuits_schema)\
               .csv("/mnt/victormrlucasformula1dl/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------


# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

def getCandidatesSchema(): 
    candidates_schema = StructType(fields=[StructField("First_Name", StringType(), False),
                                     StructField("Last_Name", StringType(), True),    
                                     StructField("Email", StringType(), True),  
                                     StructField("Application_Date", DateType(), True), 
                                     StructField("Country", StringType(), True), 
                                     StructField("YOE", IntegerType(), True), 
                                     StructField("Seniority", StringType(), True), 
                                     StructField("Technology", StringType(), True),
                                    StructField("CodeScore", DoubleType(), True),
                                    StructField("InterviewScore", DoubleType(), True)
                                   ])
    return candidates_schema  

# COMMAND ----------

df = spark.read\
               .option("header",True).option("delimiter", ";").schema(getCandidatesSchema())\
               .csv("/mnt/victormrlucasformula1dl/raw/candidates.csv")
df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/app/FullStackLabs-candidates.parquet")

print('Candidates loaded on Parquet')

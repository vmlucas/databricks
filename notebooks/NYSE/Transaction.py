# Databricks notebook source
import urllib.request
from io import StringIO
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType


# COMMAND ----------

def getStockSchema():
    stocks_schema = StructType(fields=[StructField("date", StringType(), False),
                                     StructField("stockname", StringType(), True),  
                                     StructField("quantity", DoubleType(), True), 
                                     StructField("priceUSD", DoubleType(), True), 
                                     StructField("priceBRL", DoubleType(), True), 
                                     StructField("tax", DoubleType(), True),  
                                     StructField("balanceUSD", DoubleType(), True),
                                     StructField("balanceBRL", DoubleType(), True)])
    return stocks_schema  

# COMMAND ----------

def insertDataParquet(stocks_df):    
    stocks_df.write.mode('append').parquet("/mnt/victormrlucasformula1dl/app/myNYSEstocks.parquet")

# COMMAND ----------

stocks_df = spark.read\
               .option("header",True)\
               .schema(getStockSchema())\
               .csv("/mnt/victormrlucasformula1dl/app/Oper NYSE - Sheet1.csv")
insertDataParquet(stocks_df)  
print('my stocks loaded') 
# Databricks notebook source
# MAGIC %pip install yfinance

# COMMAND ----------

spark.sql("CREATE or replace TEMPORARY VIEW STOCKS USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/myNYSEstocks.parquet\")")  
#df = spark.sql("SELECT * FROM STOCKS") 

#display(df)

# COMMAND ----------

def getTMPSchema():
    schema = StructType(fields=[StructField("stockname", StringType(), False),
                                StructField("actualPrice", DoubleType(), True)])
    return schema  
  
def getbalanceSchema():
    schema = StructType(fields=[StructField("BalanceIni", DoubleType(), False),
                                StructField("BalanceToday", DoubleType(), True),
                               StructField("Profit", DoubleType(), True)])
    return schema      

# COMMAND ----------

#getdata from yahoo
import yfinance as yf

def getActualPricesDF(df):
    data2 = []
    for row in df.collect():
        stockname = str(row["stockname"])
        stock_info = yf.Ticker(stockname).info
        market_price = stock_info['regularMarketPrice']
        data2.append((stockname,market_price))

    return data2    

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

df = spark.sql("SELECT stockname,sum(quantity) as QTD,(sum(balanceUSD)/sum(quantity)) as MPrice FROM STOCKS group by stockname order by 1") 

data2 = getActualPricesDF(df)

a = spark.createDataFrame(data=data2,schema=getTMPSchema())
final_df = df.join(a, df.stockname == a.stockname)
final_df = final_df.withColumn("BalanceIni",(F.col("MPrice")*F.col("QTD")))
final_df = final_df.withColumn("BalanceToday",(F.col("actualPrice")*F.col("QTD")))
final_df = final_df.withColumn("Profit",(F.col("actualPrice")*F.col("QTD"))-(F.col("MPrice")*F.col("QTD")))

display(final_df)
ini = final_df.agg(F.sum("BalanceIni")).collect()[0][0]
today = final_df.agg(F.sum("BalanceToday")).collect()[0][0]
profit = final_df.agg(F.sum("Profit")).collect()[0][0]
dataBalance = [(ini,today,profit)]        

balance = spark.createDataFrame(data=dataBalance,schema=getbalanceSchema())
display(balance)
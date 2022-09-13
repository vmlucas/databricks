# Databricks notebook source
import urllib.request
from io import StringIO
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType



# COMMAND ----------

def getStockSchema():
    stocks_schema = StructType(fields=[StructField("date", StringType(), False),
                                     StructField("open", DoubleType(), True),    
                                     StructField("stockname", StringType(), True),  
                                     StructField("high", DoubleType(), True), 
                                     StructField("low", DoubleType(), True), 
                                     StructField("close", DoubleType(), True), 
                                     StructField("adj_close", DoubleType(), True), 
                                     StructField("volume", DoubleType(), True)])
    return stocks_schema  

# COMMAND ----------

def insertDataParquet(stocks_df):    
    stocks_df.write.mode('append').parquet("/mnt/victormrlucasformula1dl/app/stocks.parquet")

# COMMAND ----------

import time 

def loadDataFromYahooFinance(nome,init):
  url = "https://query1.finance.yahoo.com/v7/finance/download/"+nome+"?period1="+init+"&period2="+str(int(time.time()))+"&interval=1d&events=history"
  response = urllib.request.urlopen(url)
  data = response.read()      
  text = data.decode('utf-8')  

  output = open("/dbfs/mnt/victormrlucasformula1dl/app/stocks.csv", "w", encoding='utf-8')
  output.truncate(0)
  output.write(text)
  output.close()

  stocks_df = spark.read\
               .option("header",True)\
               .schema(getStockSchema())\
               .csv("/mnt/victormrlucasformula1dl/app/stocks.csv")
  stocks_df = stocks_df.withColumn("stockname", lit(nome))
  insertDataParquet(stocks_df)  
  return nome+' stock loaded' 
#stocks_df.printSchema()
#display(stocks_df)

# COMMAND ----------

#using parquet file
stocks = ['AMD', 'AAPL','NVDA','F','AMZN','TSLA','SNAP','NIO','DIS','GOOGL','ARKK','ADM','STOR','NEM','O','SHOP','ABNB','RIVN','AUR','AMC','NU','AAL','DNA','PBR','PLTR']
#emptyRDD = spark.sparkContext.emptyRDD()
#df = spark.createDataFrame(emptyRDD,getStockSchema())
#df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/app/stocks.parquet")
global init
spark.sql("CREATE or replace TEMPORARY VIEW STOCKS USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/stocks.parquet\")")  
df = spark.sql("SELECT max(unix_timestamp(date, 'yyyy-MM-dd')) as data FROM STOCKS") 
for row in df.collect():
  init = str(row["data"])
    
for s in stocks:    
  print(loadDataFromYahooFinance(s,init))

#cursor.close()
#conn.close()
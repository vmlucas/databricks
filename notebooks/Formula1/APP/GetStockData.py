# Databricks notebook source
import urllib.request
from io import StringIO
import pymssql
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType



# COMMAND ----------

def insertData(cursor,stocks_df):
  name = ''
  stocks_entries = []   
  cont=1  
  dataCollect = stocks_df.collect()    
  for row in dataCollect:  
    name = row['stockname']
    if( cont == 1):
       cursor.execute('delete from stocks where stockname=%s',(name))
       conn.commit()
    stocks_entries.append((row['stockname'],row['date'],row['open'],row['high'],row['low'],row['close'],row['adj_close'],row['volume'])) 
    if( cont == 1000):
       cursor.executemany( "INSERT INTO stocks VALUES (%s, %s, %d,%d,%d,%d,%d,%d)",stocks_entries)
       conn.commit()
       cont =1
       stocks_entries = []
    cont = cont + 1

  cursor.executemany( "INSERT INTO stocks VALUES (%s, %s, %d,%d,%d,%d,%d,%d)",stocks_entries)
  conn.commit()
  print(name," stocks inserted")

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

def loadDataFromYahooFinance(cursor,nome):
  url = "https://query1.finance.yahoo.com/v7/finance/download/"+nome+"?period1=-2208988800&period2=1662398916&interval=1d&events=history&crumb=fPlRrNhrzGa"
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
  #insertData(cursor,stocks_df)
  insertDataParquet(stocks_df)  
  return nome+' stock loaded' 
#stocks_df.printSchema()
#display(stocks_df)

# COMMAND ----------

#using azure sql server insert
#db_server = 'vmlucas.database.windows.net'
#db_user = dbutils.secrets.get(scope="formula1-scope",key="SQL-login")
#db_pass = dbutils.secrets.get(scope="formula1-scope",key="sql-pass")
#db_name = 'DB-CASA'

#conn = pymssql.connect(db_server, db_user, db_pass, db_name)
#cursor = conn.cursor()

#using parquet file
cursor = ''
stocks = ['AMD', 'AAPL','NVDA','F','AMZN','TSLA','SNAP','NIO','BAC','CCL','ITUB','T','INTC','NOK','VALE','SHOP','SOFI','SWN','AUR','AMC','NU','AAL','DNA','PBR','PLTR']
emptyRDD = spark.sparkContext.emptyRDD()
df = spark.createDataFrame(emptyRDD,getStockSchema())
df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/app/stocks.parquet")
for s in stocks:    
  print(loadDataFromYahooFinance(cursor,s))

#cursor.close()
#conn.close()

# COMMAND ----------

spark.sql("CREATE or replace TEMPORARY VIEW STOCKS USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/stocks.parquet\")")

# COMMAND ----------

spark.sql("SELECT max(date) FROM STOCKS where stockname='NVDA'").show()
display(spark.sql("SELECT * FROM STOCKS where stockname='NVDA' and date > '2022-01-01'"))
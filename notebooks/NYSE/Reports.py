# Databricks notebook source
# MAGIC %pip install yfinance
# MAGIC %pip install pandas-datareader

# COMMAND ----------

spark.sql("CREATE or replace TEMPORARY VIEW STOCKS USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/myNYSEstocks.parquet\")")  
spark.sql("CREATE or replace TEMPORARY VIEW BALANCE USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/stocksbalance.parquet\")") #stocksbalance


# COMMAND ----------

def getTMPSchema():
    schema = StructType(fields=[StructField("stockname", StringType(), False),
                                StructField("actualPrice", DoubleType(), True)])
    return schema  
  
def getbalanceSchema():
    schema = StructType(fields=[StructField("Currency", StringType(), False),
                                StructField("BalanceIni", DoubleType(), True),
                                StructField("BalanceToday", DoubleType(), True),
                               StructField("Profit", StringType(), True)])
    return schema      

# COMMAND ----------

#getdata from yahoo
from pandas_datareader import data
from datetime import date

def getActualPricesDF(df):
   today = date.today()
   d1 = today.strftime("%Y/%m/%d")
   data2 = []
   for row in df.collect():
        stockname = str(row["stockname"])
        df = data.DataReader(stockname,'yahoo',d1)['Adj Close']
        market_price = round(float(df[0]),2)
        data2.append((stockname,market_price))

   return data2    

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pandas_datareader import data
from datetime import date

stocksDF = spark.sql("SELECT stockname,format_number(sum(quantity),2) as QTD  FROM STOCKS group by stockname having QTD >=1 order by 1")
mpriceDF = spark.sql("SELECT stockname,format_number((sum(priceUSD)/sum(quantity)),2) as MPrice FROM STOCKS where quantity >=0 group by stockname order by 1")

final_df = stocksDF.alias('s').join(mpriceDF, stocksDF.stockname == mpriceDF.stockname,"left")
final_df = final_df.select(F.col("s.stockname"),F.col("QTD"),F.col("MPrice"))

balCashDF = spark.sql("SELECT round(sum(ValorUSD),2) as usd FROM BALANCE ")
balStocksDF = spark.sql("SELECT round(sum(balanceUSD),2) as usd FROM STOCKS ")
cash = balCashDF.collect()[0][0] + balStocksDF.collect()[0][0]
print(cash)


data2 = getActualPricesDF(final_df)
a = spark.createDataFrame(data=data2,schema=getTMPSchema())
final_df = final_df.alias('s').join(a, final_df.stockname == a.stockname)
final_df = final_df.withColumn("BalanceIni",F.format_number((F.col("MPrice")*F.col("QTD")),2))
final_df = final_df.withColumn("BalanceToday",F.format_number((F.col("actualPrice")*F.col("QTD")),2))
final_df = final_df.withColumn("Profit",F.format_number((F.col("actualPrice")*F.col("QTD"))-(F.col("MPrice")*F.col("QTD")),2))
final_df = final_df.select(F.col("s.stockname"),F.col("QTD"),F.col("MPrice"),F.col("actualPrice"),F.col("BalanceIni"),F.col("BalanceToday"),F.col("Profit"))

display(final_df)
stockCosts = (final_df.agg(F.sum("BalanceIni")).collect()[0][0])
ini = stockCosts + cash 
actual = (final_df.agg(F.sum("BalanceToday")).collect()[0][0])+cash
profit = (final_df.agg(F.sum("Profit")).collect()[0][0])+cash
strProfit = f'{profit:9.2f}'

today = date.today()

# dd/mm/YY
d1 = today.strftime("%Y/%m/%d")
df = data.DataReader('USDBRL=X','yahoo',d1)['Adj Close']
iniBRL = round(float(ini*df[0]),2)
actualBRL = round(float(actual*df[0]),2)
profitBRL = float(profit*df[0])
strProfitBRL = f'{profitBRL:9.2f}'
dataBalance = [('USD',ini,actual,strProfit),('BRL',iniBRL,actualBRL,strProfitBRL)]        

balance = spark.createDataFrame(data=dataBalance,schema=getbalanceSchema())
display(balance)
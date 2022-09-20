# Databricks notebook source
spark.sql("CREATE or replace TEMPORARY VIEW STOCKS USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/stocks.parquet\")")

# COMMAND ----------

#spark.sql("SELECT max(date) FROM STOCKS where stockname='ARKK'").show()
#spark.sql("SELECT count(*) FROM STOCKS where stockname='AAPL'").show()

display(spark.sql("SELECT * FROM STOCKS where stockname='AAPL' and date > '2022-01-01'"))
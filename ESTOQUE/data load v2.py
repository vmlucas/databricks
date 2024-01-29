# Databricks notebook source
# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/shared_uploads/victormrlucas@hotmail.com/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(to_date(data,"dd/MM/yyyy"))  from estoque.estoque_cm;
# MAGIC select max(data) FROM estoque.valor_cm

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *

estoqueDF = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/victormrlucas@hotmail.com/estoque_20240123___estoque.csv")
estoqueDF = estoqueDF.fillna(0)
estoqueDF = estoqueDF.withColumn("QTD",f.col("QTD").cast(DoubleType()))
estoqueDF = estoqueDF.select(f.col("NOME"),f.col("QTD"),f.col("DATA"),f.col("TIPO"))

valorDF = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/victormrlucas@hotmail.com/estoque_20240123___valor.csv")
valorDF = valorDF.fillna(0.0)
valorDF = valorDF.withColumn("valor_unidade",f.col("valor_unidade").cast(DoubleType()))
valorDF = valorDF.withColumn("DATA", f.to_date(valorDF["DATA"], "dd/MM/yyyy").cast(DateType()))
valorDF = valorDF.select(f.col("NOME"),f.col("DATA"),f.col("valor_unidade"))

#appending data
estoqueDF.write.mode("append").saveAsTable("estoque.estoque_CM")
valorDF.write.mode("append").saveAsTable("estoque.valor_CM")

print("dados salvos")

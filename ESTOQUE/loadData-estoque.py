# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType,ArrayType,FloatType
import pyspark.sql.functions as f

def getSchema():  
     schema = StructType(fields=[StructField("NOME", StringType(), False),
                                     StructField("QTD", StringType(), True),    
                                     StructField("DATA", StringType(), True),  
                                     StructField("TIPO", StringType(), True)])
     return schema 
    
def getValorSchema():  
     schema = StructType(fields=[StructField("NOME", StringType(), False),
                                 StructField("DATA", StringType(), True),  
                                     StructField("valor_unidade", StringType(), True)])
     return schema     

# COMMAND ----------

import pandas as pd 
import numpy as np
#read common file
files = dbutils.fs.ls('/mnt/victormrlucasformula1dl/cmrj/estoque/raw')
for fi in files: 
    arquivo = fi.name
    print(arquivo)
    if not arquivo.startswith("valor"):
      df = pd.read_csv('/dbfs/mnt/victormrlucasformula1dl/cmrj/estoque/raw/'+arquivo)
      df = df.replace(np.nan, ' ')
      columns = df.columns
      dados = []
      for index, row in df.iterrows():
        if 'DATA1' in columns:
           qtd = row['QTD1']
           data = row['DATA1']   
           tipo = row['TIPO']
           dados.append((row['NOME'],qtd,data,tipo)) 
        if 'DATA2' in columns:
           qtd = row['QTD2']
           data = row['DATA2']   
           tipo = row['TIPO']
           dados.append((row['NOME'],qtd,data,tipo)) 
        if 'DATA3' in columns:  
           qtd = row['QTD3']
           data = row['DATA3']   
           tipo = row['TIPO']
           dados.append((row['NOME'],qtd,data,tipo))  
        if 'DATA4' in columns:  
           qtd = row['QTD4']
           data = row['DATA4']   
           tipo = row['TIPO']
           dados.append((row['NOME'],qtd,data,tipo)) 
        if 'DATA5' in columns:  
           qtd = row['QTD5']
           data = row['DATA5']   
           tipo = row['TIPO']
           dados.append((row['NOME'],qtd,data,tipo))   
      estoqueDF = spark.createDataFrame(dados,getSchema())
      estoqueDF = estoqueDF.fillna(0)
      estoqueDF = estoqueDF.withColumn("QTD",f.col("QTD").cast(IntegerType()))
      estoqueDF.show()
      
      estoqueDF.write.mode("append").saveAsTable("estoque_CM")
      estoqueDF.write.mode('append').parquet("/mnt/victormrlucasformula1dl/app/estoque.parquet") 
       
      #cmrj/estoque/raw estoque/processed/
      dbutils.fs.mv("/mnt/victormrlucasformula1dl/cmrj/estoque/raw/"+arquivo, "/mnt/victormrlucasformula1dl/cmrj/estoque/processed/"+arquivo)  
      #dbutils.fs.rm("/mnt/victormrlucasformula1dl/estoque-raw/"+arquivo) 

# COMMAND ----------

import pandas as pd 
import numpy as np
#read valor file
files = dbutils.fs.ls('/mnt/victormrlucasformula1dl/cmrj/estoque/raw')
for fi in files: 
    arquivo = fi.name
    print(arquivo)
    if arquivo.startswith("valor"):
      df = pd.read_csv('/dbfs/mnt/victormrlucasformula1dl/cmrj/estoque/raw/'+arquivo)
      df = df.replace(np.nan, ' ')
      columns = df.columns
      dados = []
      for index, row in df.iterrows():
           valor = row['valor_unidade']
           data = row['DATA']   
           dados.append((row['NOME'],data,valor)) 
      valorDF = spark.createDataFrame(dados,getValorSchema())
      valorDF = valorDF.fillna(0)
      valorDF = valorDF.withColumn("valor_unidade",f.col("valor_unidade").cast(FloatType()))
      valorDF.show()
      
      valorDF.write.mode("append").saveAsTable("valor_CM")
      valorDF.write.mode('append').parquet("/mnt/victormrlucasformula1dl/app/valor-estoque.parquet") 
       
      dbutils.fs.mv("/mnt/victormrlucasformula1dl/cmrj/estoque/raw/"+arquivo, "/mnt/victormrlucasformula1dl/cmrj/estoque/processed/"+arquivo)  
      #dbutils.fs.rm("/mnt/victormrlucasformula1dl/estoque-raw/"+arquivo) 

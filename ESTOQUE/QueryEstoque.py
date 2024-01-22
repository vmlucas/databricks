# Databricks notebook source
#spark.sql("CREATE or replace TEMPORARY VIEW ESTOQUE USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/estoque.parquet\")")
#spark.sql("CREATE or replace TEMPORARY VIEW VALOR USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/valor-estoque.parquet\")")

# COMMAND ----------

#df = spark.sql("SELECT * FROM VALOR")
#table_name = "valor_CM"
#df.write.saveAsTable(table_name)

#dfe = spark.sql("SELECT * FROM ESTOQUE")
#table_name = "estoque_CM"
#dfe.write.saveAsTable(table_name)

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as f

valor_df = spark.read.table("estoque.valor_CM")
valor_df = valor_df.withColumn("DATA",f.to_date(f.col("DATA"),"dd/MM/yyyy")) 
#agg_df = valor_df.groupBy('nome').agg(f.max('valor_unidade'))
agg_df = valor_df.groupBy('nome').agg(f.max('DATA'))

display(agg_df)

# COMMAND ----------

import numpy as np

estoque_df = spark.read.table("estoque.estoque_CM")
estoque_df = estoque_df.withColumn("DATA",f.to_date(f.col("DATA"),"dd/MM/yyyy")) 
nomeDF = estoque_df.dropDuplicates(["nome"]).filter(f.length(f.trim(f.col("nome"))) > 0).select("nome").orderBy("nome")
rows = nomeDF.select("NOME").collect()
nomes=[]
for r in rows:
    nomes.append(r['NOME'])
dbutils.widgets.dropdown("remedio", "ABIRATERONA 250MG", nomes)

# COMMAND ----------

print('QTD Estoque')
remedio = dbutils.widgets.get("remedio")
#spark.sql("SELECT * FROM ESTOQUE").show()
tempDF = estoque_df.filter((f.col("QTD") >=0) & (f.col("nome")==remedio)).orderBy("nome","data")
painelDF = tempDF.alias("a").join(valor_df.alias("b"),(f.col("a.NOME") == f.col("b.NOME")) & (f.col("a.DATA") == f.col("b.DATA")),"left")\
        .drop(f.col("b.NOME")).drop(f.col("b.DATA"))
painelDF = painelDF.withColumn("valor_unidade",f.when(f.col("valor_unidade").isNull() ,0)\
                                               .otherwise(f.col("valor_unidade")))        
painelDF = painelDF.orderBy("DATA")        
display(painelDF)


# COMMAND ----------

print('Valor Estoque')
#spark.sql("SELECT * FROM ESTOQUE").show()
dataVal = estoque_df.select(f.max(f.col("data"))).first()[0]
tempDF2 = estoque_df.filter((f.col("QTD") >=0) & (f.col("data")==dataVal))
painelDF = tempDF2.alias("a").join(valor_df.alias("b"),(f.col("a.NOME") == f.col("b.NOME")) & (f.col("a.DATA") == f.col("b.DATA")),"left")\
        .drop(f.col("b.NOME")).drop(f.col("b.DATA"))

painelDF = painelDF.withColumn("valor_Atual",f.when(f.col("valor_unidade").isNull() ,0)\
                                               .otherwise(f.col("qtd")*f.col("valor_unidade")))        
painelDF = painelDF.orderBy("DATA") 
agg_df = painelDF.groupBy('tipo').agg(f.sum('valor_Atual')).orderBy("tipo")       
display(agg_df)


# COMMAND ----------

print('Valor Estoque x tempo')
#spark.sql("SELECT * FROM ESTOQUE").show()
tempDF2 = estoque_df.filter(f.col("QTD") >=0 )
painelDF = tempDF2.alias("a").join(valor_df.alias("b"),(f.col("a.NOME") == f.col("b.NOME")) & (f.col("a.DATA") == f.col("b.DATA")),"left")\
        .drop(f.col("b.NOME")).drop(f.col("b.DATA"))

painelDF = painelDF.withColumn("valor",f.when(f.col("valor_unidade").isNull() ,0)\
                                               .otherwise(f.col("qtd")*f.col("valor_unidade")))        
agg_df = painelDF.groupBy('DATA').agg(f.sum('valor')).orderBy("DATA") 
agg_df = agg_df.filter(f.col("sum(valor)") > 0 )      
display(agg_df)


# COMMAND ----------

print('QTD Estoque')
tempDF2 = estoque_df.filter(f.col("QTD") >=0 )
agg_df = tempDF2.groupBy('TIPO','DATA').agg(f.sum('QTD')).orderBy("TIPO","DATA") 
display(agg_df)

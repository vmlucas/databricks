# Databricks notebook source
def format_serie(data):   
    if "Agendamento_Aplicacao" in data:
       return {
          "_id" : str(data['_id']),
          "CPF" : data['CPF'],
          "Atendimento_Inicial" : data['Atendimento_Inicial'],
          "Medicamento" : data['Medicamento'],
          "Nome" : data['Nome'],
          "Origem" : data['Origem'],
          "Processo_Judicial" : data['Processo_Judicial'],
          "Agendamento_Aplicacao" : data['Agendamento_Aplicacao']
      }
    else:
       return {
          "_id" : str(data['_id']),
          "CPF" : data['CPF'],
          "Atendimento_Inicial" : data['Atendimento_Inicial'],
          "Medicamento" : data['Medicamento'],
          "Nome" : data['Nome'],
          "Origem" : data['Origem'],
          "Processo_Judicial" : data['Processo_Judicial']
      } 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType,ArrayType

def getPacientesSchema():
     pacientes_schema = StructType(fields=[StructField("_id", StringType(), False),
                                     StructField("CPF", StringType(), True),    
                                     StructField("Nome", StringType(), True),  
                                     StructField("Medicamento", StringType(), True), 
                                     StructField("Origem", StringType(), True)])
     return pacientes_schema   
    

# COMMAND ----------

#load pacientes
pacientes_df = spark.read\
                     .format("mongo")\
                     .schema(getPacientesSchema())\
                     .options(database="Pacientes-SESRJ", collection="HUPE").load()

#display(pacientes_df)
pacientes_df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/app/pacientes.parquet") 

print('Pacientes HUPE loaded')

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F

#load atendimentos
pipeline="[{'$unwind': { path: '$Atendimento_Inicial' }},\
  {'$project': {_id:1,Nome:1,'Atendimento_Inicial.Medicamento':1,'Atendimento_Inicial.Agendado':1,'Atendimento_Inicial.Indicacao':1,\
    'Atendimento_Inicial.Data_Atendimento':1,'Atendimento_Inicial.OBS':1,'Atendimento_Inicial.Laudo':1}}]"

atends_df = spark.read.format("mongo").options(database="Pacientes-SESRJ", collection="HUPE").option("pipeline", pipeline).load()

atends_df = atends_df.select(F.col('_id').getItem('oid').alias('id'), 'Nome', F.col('Atendimento_Inicial').getItem('Medicamento').alias('Medicamento'),\
                            F.col('Atendimento_Inicial').getItem('Agendado').alias('Agendado'),\
                            F.col('Atendimento_Inicial').getItem('Indicacao').alias('Indicacao'),\
                            F.col('Atendimento_Inicial').getItem('Data_Atendimento').alias('Data_Atendimento'),
                            F.col('Atendimento_Inicial').getItem('OBS').alias('OBS'),
                            F.col('Atendimento_Inicial').getItem('Laudo').alias('Laudo'))

atends_df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/app/atendimentos-hupe.parquet") 

print('Atendimentos HUPE loaded')

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F

#load aplicacoes
pipeline="[{'$unwind': { path: '$Agendamento_Aplicacao' }},\
  {'$project': {_id:1,Nome:1,'Agendamento_Aplicacao.Medicamento':1,'Agendamento_Aplicacao.Indicacao':1,\
    'Agendamento_Aplicacao.Data':1,'Agendamento_Aplicacao.OBS':1}}]"

aplics_df = spark.read.format("mongo").options(database="Pacientes-SESRJ", collection="HUPE").option("pipeline", pipeline).load()

aplics_df = aplics_df.select(F.col('_id').getItem('oid').alias('id'), 'Nome', F.col('Agendamento_Aplicacao').getItem('Medicamento').alias('Medicamento'),\
                            F.col('Agendamento_Aplicacao').getItem('Indicacao').alias('Indicacao'),\
                            F.col('Agendamento_Aplicacao').getItem('Data').alias('Data'),
                            F.col('Agendamento_Aplicacao').getItem('OBS').alias('OBS'))

aplics_df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/app/aplicacoes-hupe.parquet") 

print('Aplicacoes HUPE loaded')
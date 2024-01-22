# Databricks notebook source
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType,ArrayType

def format_hupe(data):   
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



def getSchema():
     notas_schema = StructType(fields=[StructField("_id", StringType(), False),
                                     StructField("Nome", StringType(), True),    
                                     StructField("Matrícula", StringType(), True),  
                                     StructField("Turno", StringType(), True), 
                                     StructField("Data Nascimento", StringType(), True),
                                     StructField("No Chamada", IntegerType(), True),
                                     StructField("Tipo Curso", StringType(), True),
                                     StructField("Turma", StringType(), True),
                                     StructField("Ano Letivo", IntegerType(), True)])
     return notas_schema  
    

# COMMAND ----------

#load notas
notas_df = spark.read\
                     .format("mongo")\
                     .schema(getSchema())\
                     .options(database="HOME", collection="Notas").load()

notas_df = notas_df.select(F.col('_id').alias('id'),F.col('Nome'),F.col('Matrícula').alias("Matricula"),F.col('Turno'),F.col('Data Nascimento').alias("Data_Nascimento"),\
                            F.col('No Chamada').alias("No_Chamada"),F.col('Tipo Curso').alias("Tipo_Curso"),F.col('Turma'),F.col('Ano Letivo').alias("Ano_Letivo"))
notas_df.show(truncate=False)                            
notas_df.write.mode("overwrite").saveAsTable("casa.notas_vic")
print('Notas Vic loaded')

# COMMAND ----------

#load atendimentos
pipeline="[{'$unwind': { path: '$Disciplinas' }},\
  {'$project': {_id:1,Turma:1,'Disciplinas.Nome':1,'Disciplinas.Notas':1}}]"

discip_df = spark.read.format("mongo").options(database="HOME", collection="Notas").option("pipeline", pipeline).load()

discip_df = discip_df.select(F.col('_id').getItem('oid').alias('id'), 'Turma', F.col('Disciplinas').getItem('Nome').alias('Disciplina'),\
                            F.posexplode(F.col('Disciplinas').getItem('Notas').getItem('Nota')))
discip_df = discip_df.select(F.col('id'),F.col('Turma'),F.col('Disciplina'),F.col('pos').alias("Bimestre"),\
                            F.col('col').alias("Nota"))
discip_df = discip_df.withColumn("Bimestre",F.col("Bimestre")+F.lit(1))                            
#atends_df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/app/atendimentos-hupe.parquet") 
discip_df.write.mode("overwrite").saveAsTable("casa.disciplinas_vic")
discip_df.show(truncate=False)
print('Disciplinas loaded')

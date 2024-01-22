# Databricks notebook source
'''spark.sql("CREATE or replace TEMPORARY VIEW PACIENTES USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/pacientes.parquet\")")
spark.sql("CREATE or replace TEMPORARY VIEW ATENDIMENTOS USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/atendimentos-hupe.parquet\")")
spark.sql("CREATE or replace TEMPORARY VIEW APLICS USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/app/aplicacoes-hupe.parquet\")")'''

pacientes_df = spark.read.table("pacientes_hupe")
pacientes_df.createOrReplaceTempView("PACIENTES")

atend_df = spark.read.table("atendimentos_hupe")
atend_df.createOrReplaceTempView("ATENDIMENTOS")

aplic_df = spark.read.table("aplicacoes_hupe")
aplic_df.createOrReplaceTempView("APLICS")

# COMMAND ----------

print('Pacientes primeiro atendimento com indicacao')
spark.sql("SELECT count(*) FROM PACIENTES").show()
#spark.sql("SELECT count(*) FROM ATENDIMENTOS").show()
display(spark.sql("SELECT /*p.Nome*/count( distinct p.CPF) as Pacientes_Primeiro_Atend_com_Indicacao , substr(a.Data_Atendimento,1,7) as Meses /*,a.Indicacao,a.Agendado*/ \
                            FROM PACIENTES p,ATENDIMENTOS a where p._id = a.id and a.Indicacao like 'SIM%' /*and a.Data_Atendimento >= '2022-01-01'*/ group by substr(a.Data_Atendimento,1,7) order by 2 desc"))
#display(spark.sql("SELECT p.Nome,p.CPF,a.Data,a.Indicacao,a.OBS FROM PACIENTES p,APLICS a where p._id = a.id and upper(p.Nome) like '%INHA%' order by 1,3"))

# COMMAND ----------

print('Aplicacoes em Pacientes distintos por mes')
display(spark.sql("SELECT /*p.Nome*/count( distinct p.CPF) as Aplicacoes_em_Pacientes_Distintos, substr(a.Data,1,7) as Meses/*,a.Indicacao,a.Agendado*/ \
                    FROM PACIENTES p,APLICS a where p._id = a.id and  a.Indicacao like 'SIM%' /*and a.Data_Atendimento >= '2022-01-01'*/ group by substr(a.Data,1,7) order by 2 desc"))

# COMMAND ----------

display(spark.sql("SELECT /*p.Nome*/count( distinct p.CPF) as Pacientes_Primeiro_Atend_sem_Indicacao , substr(a.Data_Atendimento,1,7) as Meses /*,a.Indicacao,a.Agendado*/ \
                            FROM PACIENTES p,ATENDIMENTOS a where p._id = a.id and a.Indicacao like 'SEM%' /*and a.Data_Atendimento >= '2022-01-01'*/ group by substr(a.Data_Atendimento,1,7) order by 2 desc"))
#display(spark.sql("SELECT p.Nome,p.CPF,a.Data,a.Indicacao,a.OBS FROM PACIENTES p,APLICS a where p._id = a.id and upper(p.Nome) like '%INHA%' order by 1,3"))

# COMMAND ----------

display(spark.sql("SELECT /*p.Nome*/count( distinct p.CPF) as Pacientes_Primeiro_Atend_nao_compareceram , substr(a.Data_Atendimento,1,7) as Meses /*,a.Indicacao,a.Agendado*/ \
                            FROM PACIENTES p,ATENDIMENTOS a where p._id = a.id and a.Indicacao like 'NÃO%' /*and a.Data_Atendimento >= '2022-01-01'*/ group by substr(a.Data_Atendimento,1,7) order by 2 desc"))
#display(spark.sql("SELECT p.Nome,p.CPF,a.Data,a.Indicacao,a.OBS FROM PACIENTES p,APLICS a where p._id = a.id and upper(p.Nome) like '%INHA%' order by 1,3"))

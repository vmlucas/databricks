# Databricks notebook source
# MAGIC %md
# MAGIC <table style="background-color:#0b3e7d;">
# MAGIC   <tr>
# MAGIC      <td>       
# MAGIC        <a href="https://charts.mongodb.com/charts-project-0-ohubx/public/dashboards/a5ca7e6b-efd7-4340-9cf4-5cf2d446d4a9" target="_blank">Dashboard Dados HUPE</a>
# MAGIC      </td>
# MAGIC      <td>
# MAGIC        <a href="https://charts.mongodb.com/charts-project-0-ohubx/public/dashboards/2964b645-cf3a-4105-b010-8a5c911b77ff" target="_bank">Tabelas com Dados Consolidados</a>
# MAGIC      </td>
# MAGIC      <td>
# MAGIC        <a href="https://adb-1072724438930721.1.azuredatabricks.net/?o=1072724438930721#notebook/4355907813484990/dashboard/3292780618489054/present" target="_blank">Graficos Dados HUPE</a></td>
# MAGIC   </tr></table>     
# MAGIC          

# COMMAND ----------

dbutils.widgets.text("Nome", "", label="Nome")
dbutils.widgets.text("CPF", "", label="CPF")
dbutils.widgets.text("Data Atendimento", "", label="Data Atendimento(dd/mm/aaaa)")

# COMMAND ----------

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

cpf = dbutils.widgets.get("CPF")
nome = dbutils.widgets.get("Nome")
data = dbutils.widgets.get("Data Atendimento")
print(data)
if(data != ''):
    display(spark.sql("SELECT p.Nome, p.cpf, p.origem,a.Medicamento, a.Agendado, a.Indicacao, a.Data_Atendimento, a.OBS, a.Laudo \
                         FROM PACIENTES p,ATENDIMENTOS a where p._id = a.id and to_date(a.Data_Atendimento,'dd/MM/yyyy') = to_date('{0}','dd/MM/yyyy') \
                               and upper(p.Nome) like upper('%{1}%') order by 1,5".format(data,nome)))     
else:
    if(nome != ''):
        display(spark.sql("SELECT p.Nome, p.cpf, p.origem,a.Medicamento, a.Agendado, a.Indicacao, a.Data_Atendimento, a.OBS, a.Laudo \
                         FROM PACIENTES p,ATENDIMENTOS a where p._id = a.id and upper(p.Nome) like upper('%{0}%') order by 1,5".format(nome)))
    if(cpf != ''):
        display(spark.sql("SELECT p.Nome, p.cpf, p.origem,a.Medicamento, a.Agendado, a.Indicacao, a.Data_Atendimento, a.OBS, a.Laudo \
                         FROM PACIENTES p,ATENDIMENTOS a where p._id = a.id and p.CPF ='{0}' order by 1,5".format(cpf)))     
           

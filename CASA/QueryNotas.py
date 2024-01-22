# Databricks notebook source
notas_df = spark.read.table("casa.notas_vic")
notas_df.createOrReplaceTempView("notas")

df = spark.read.table("casa.disciplinas_vic")
df.createOrReplaceTempView("discip")


# COMMAND ----------

turmaDF = df.dropDuplicates(["Turma"])
rows = turmaDF.select("Turma").collect()
turmas=[]
turmas.append("Todas")
for r in rows:
    turmas.append(r['Turma'])
dbutils.widgets.dropdown("Turmas", "Todas", turmas)

discDF = df.dropDuplicates(["Disciplina"])
rows = discDF.select("Disciplina").collect()
disc=[]
disc.append("Todas")
for r in rows:
    disc.append(r['Disciplina'])
disc.sort()    
dbutils.widgets.dropdown("Disciplinas", "Todas", disc)

# COMMAND ----------

widT = dbutils.widgets.get("Turmas")
widD = dbutils.widgets.get("Disciplinas")
if(widT != 'Todas'):
    if(widD != 'Todas'):
        display(spark.sql("SELECT Turma, Disciplina, Bimestre, Nota \
                            FROM discip where Turma='{0}' and Disciplina='{1}' order by 2,3".format(widT,widD)))
    else:
        display(spark.sql("SELECT Turma, Disciplina, Bimestre, Nota \
                            FROM discip where Turma='{0}' order by 2,3".format(widT)))    

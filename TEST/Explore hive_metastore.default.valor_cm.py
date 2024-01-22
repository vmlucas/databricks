# Databricks notebook source
# MAGIC %sql 
# MAGIC SELECT count(*) FROM hupe.atendimentos_hupe;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(to_date(data,"dd/MM/yyyy")) FROM estoque.estoque_cm t;
# MAGIC
# MAGIC SELECT max(to_date(data,"dd/MM/yyyy")) FROM estoque.valor_cm t;
# MAGIC
# MAGIC --SELECT DISTINCT to_date(t.data,"dd/MM/yyyy") FROM default.valor_cm t where substr(data,4,10) ='09/2023' ORDER BY 1
# MAGIC    -- where to_date(t.data,"dd/MM/yyyy") = (select max(to_date(data,"dd/MM/yyyy")) from default.estoque_cm)
# MAGIC     --order by t.NOME

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM default.valor_cm t where data ='11/09/2023' ORDER BY 1
# MAGIC
# MAGIC --delete FROM default.estoque_cm where data ='11/09/2023'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`valor_cm` t where to_date(t.data,"dd/MM/yyyy") = (select max(to_date(data,"dd/MM/yyyy")) from valor_cm)

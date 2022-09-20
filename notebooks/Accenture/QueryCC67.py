# Databricks notebook source
spark.sql("CREATE or replace TEMPORARY VIEW CC67_TEL USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/accenture/CC67_tel1_tmp.parquet\")")
spark.sql("CREATE or replace TEMPORARY VIEW DM_SUBSCRICAO USING parquet OPTIONS (path \"/mnt/victormrlucasformula1dl/accenture/dm_subscricao.parquet\")")

# COMMAND ----------

display(spark.sql("SELECT count(*) \
                           FROM CC67_TEL "))
display(spark.sql("SELECT count(*) \
                           FROM DM_SUBSCRICAO"))
df = spark.sql("SELECT * \
                           FROM DM_SUBSCRICAO")

# COMMAND ----------

from pyspark.sql.functions import col

display(df.select(col("NU_SUBSCRICAO"),col("DT_INI_SIT_SUBSCRICAO"),col("NO_TIPO_SIT_SUBSCRICAO"),col("CD_SUBSCRICAO_SIST_ORIG"),col("DS_OFERTA_PROMO_SERV_ASS")).where("NU_SUBSCRICAO is Null"))

# COMMAND ----------

#ETL Process
df = spark.sql("SELECT A.PROTOCOLO,\
                                   A.DT_ABERTURA,\
                                   A.TELRECLAMADO,\
                                   B.NO_TIPO_SIT_SUBSCRICAO,\
                                   B.CD_SUBSCRICAO_SIST_ORIG,\
                                   B.DS_OFERTA_PROMO_SERV_ASS,\
                                   B.NU_SUBSCRICAO,\
                                   B.DT_INI_SIT_SUBSCRICAO\
                           FROM CC67_TEL A \
                             inner join DM_SUBSCRICAO B on A.TELRECLAMADO = concat(B.NU_AREA_TELEFONE,B.NU_LINHA_TELEFONE)")
                           #where B.NO_TIPO_SUBSCRICAO = 'LINHA MOVEL'")

display(df)
# Databricks notebook source
import urllib.request
from io import StringIO
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType


# COMMAND ----------

def getTelSchema():
    tel_schema = StructType(fields=[StructField("ANOMES", StringType(), False),
                                     StructField("PROTOCOLO", StringType(), True),    
                                     StructField("DT_ABERTURA", StringType(), True),  
                                     StructField("PRODUTO", StringType(), True), 
                                     StructField("PROBLEMA", StringType(), True), 
                                     StructField("CPFCNPJCONSUMIDOR", StringType(), True), 
                                     StructField("UFASSINANTE", StringType(), True), 
                                     StructField("CIDADEASSINANTE", StringType(), True),
                                    StructField("DDD_CONSUMIDOR", StringType(), True),
                                    StructField("TELRECLAMADO", StringType(), True)
                                   ])
    return tel_schema  

# COMMAND ----------

def getDMSubscricaoSchema():
    schema =  StructType(fields=[StructField("NU_SUBSCRICAO", StringType(), False),
                                 StructField("CD_SUBSCRICAO_SIST_ORIG", StringType(), True),
                                 StructField("NO_TIPO_SUBSCRICAO", StringType(), True),
                                 StructField("NO_TIPO_SIT_SUBSCRICAO", StringType(), True),
                                 StructField("NO_PAPEL_REL_SUBSCRICAO", StringType(), True),
                                 StructField("NO_CENTRO_DECISAO", StringType(), True),
                                 StructField("NO_GRUPO_OPER_TELECOM_ORIG", StringType(), True),
                                 StructField("NO_CURTO_TERRITORIO_OPERADORA", StringType(), True),
                                 StructField("NU_AREA_TELEFONE", StringType(), True),
                                 StructField("NU_LINHA_TELEFONE", StringType(), True),
                                 StructField("NO_REGIAO", StringType(), True),
                                 StructField("NU_AREA_TELEFONE_PRINCIPAL", StringType(), True),
                                 StructField("NU_LINHA_TELEFONE_PRINCIPAL", StringType(), True),
                                 StructField("NU_SUBSCRICAO_PRINCIPAL", StringType(), True),
                                 StructField("DT_INSTALACAO", StringType(), True),
                                 StructField("CD_LOCALIDADE_PONTA_A", StringType(), True),
                                 StructField("NO_MOTIVO_RETIRADA", StringType(), True),
                                 StructField("DT_MIGRACAO", StringType(), True),
                                 StructField("DT_ULTIMA_INSTALACAO", StringType(), True),
                                 StructField("DT_ATIVACAO", StringType(), True),
                                 StructField("DT_CONTRATACAO", StringType(), True),
                                 StructField("DT_CANCELAMENTO", StringType(), True),
                                 StructField("DT_ULTIMA_MIGRACAO_PROD", StringType(), True),
                                 StructField("DT_INI_SIT_SUBSCRICAO", StringType(), True),
                                 StructField("NO_VL_TIPO_SIT_SUBSCRICAO", StringType(), True),
                                 StructField("NO_MOTIVO_SIT_SUBSCRICAO", StringType(), True),
                                 StructField("DT_VENDA", StringType(), True),
                                 StructField("DT_EXPIRACAO_CONTRATO", StringType(), True),
                                 StructField("NO_PESSOA_PAGADORA", StringType(), True),
                                 StructField("CD_IDENT_PESSOA_PAGADORA", StringType(), True),
                                 StructField("NO_CONGL_PESSOA_PAGADORA", StringType(), True),
                                 StructField("NO_PESSOA_USUARIA", StringType(), True),
                                 StructField("CD_IDENT_PESSOA_USUARIA", StringType(), True),
                                 StructField("NO_TIPO_IDENT_PESSOA_USUARIA", StringType(), True),
                                 StructField("NU_CONTA_UNIF_PESSOA_USUARIA", StringType(), True),
                                 StructField("NO_CONGL_PESSOA_USUARIA", StringType(), True),
                                 StructField("NO_HOLDING_PESSOA_USUARIA", StringType(), True),
                                 StructField("NO_CATEGORIA_LINHA_FIXA", StringType(), True),
                                 StructField("CD_PLANO_BILHETAGEM", StringType(), True),
                                 StructField("IN_31_ANOS", StringType(), True),
                                 StructField("NO_SEG_CLASSIFIC_LINHA", StringType(), True),
                                 StructField("NU_INST_EQUIP_COMUTACAO", StringType(), True),
                                 StructField("SG_ESTACAO", StringType(), True),
                                 StructField("SG_AREA_GER", StringType(), True),
                                 StructField("NO_AREA_CONCESSAO", StringType(), True),
                                 StructField("NO_PERFIL_CARTAO_SIM", StringType(), True),
                                 StructField("NO_ATIVIDADE_LISTA_TELEF", StringType(), True),
                                 StructField("DS_REF_ENDERECO_COBRANCA", StringType(), True),
                                 StructField("DS_REF_ENDERECO_INSTALACAO", StringType(), True),
                                 StructField("NO_PROD_EQUIP", StringType(), True),
                                 StructField("NO_VL_TIPO_INST_PROD_EQUIP", StringType(), True),
                                 StructField("NO_COR_PROD_EQUIP", StringType(), True),
                                 StructField("NO_FABRICANTE_PROD_EQUIP", StringType(), True),
                                 StructField("NU_DESIGNACAO", StringType(), True),
                                 StructField("DS_VELOCIDADE", StringType(), True),
                                 StructField("DS_OFERTA_PROMO_SERV_ASS", StringType(), True),
                                 StructField("IN_PORTNUM", StringType(), True),
                                 StructField("NO_ABRANGENCIA_PROD_CIRCUITO", StringType(), True),
                                 StructField("NO_GRUPO_PORTNUM", StringType(), True),
                                 StructField("NO_TIPO_PORTNUM", StringType(), True),
                                 StructField("NU_OPERADORA_DOADORA", StringType(), True),
                                 StructField("NO_OPERADORA_DOADORA", StringType(), True),
                                 StructField("NU_OPERADORA_RECEPTORA", StringType(), True),
                                 StructField("NO_OPERADORA_RECEPTORA", StringType(), True),
                                 StructField("NO_LOCALIDADE_ANTERIOR", StringType(), True),
                                 StructField("NO_LOCALIDADE_ATUAL", StringType(), True),
                                 StructField("IN_SUSBCRICAO_USUARIA_3G", StringType(), True),
                                 StructField("DT_PORTABILIDADE", StringType(), True),
                                 StructField("DS_FRANQUIA_DADOS_3G", StringType(), True),
                                 StructField("NO_LOGIN_PRIMEIRA_NAVEGACAO", StringType(), True),
                                 StructField("DT_INI_PRIMEIRA_NAVEGACAO", StringType(), True),
                                 StructField("HR_INI_PRIMEIRA_NAVEGACAO", StringType(), True),
                                 StructField("QT_PONTO_ADICIONAL", StringType(), True),
                                 StructField("QT_CANAL_AVULSO", StringType(), True),
                                 StructField("DS_CONTRATO", StringType(), True),
                                 StructField("CD_MATRICULA_ATENDENTE", StringType(), True),
                                 StructField("CD_EVENTO_SIST_ORIG", StringType(), True),
                                 StructField("DS_FRANQUIA_SUBSCRICAO", StringType(), True),
                                 StructField("DS_PERFIL_TARIFARIO", StringType(), True),
                                 StructField("DT_FIM_CONTR_FIDELIZACAO_SMP", StringType(), True),
                                 StructField("DT_FIM_CONTR_DOWNGRADE_PLANO", StringType(), True),
                                 StructField("DT_INI_FIDELIZACAO_SUBSC_EQUIP", StringType(), True),
                                 StructField("DT_FIM_FIDELIZACAO_SUBSC_EQUIP", StringType(), True),
                                 StructField("DS_PERFIL_TARIFARIO_SUBSCRICAO", StringType(), True),
                                 StructField("IN_AVISO_CONSUMO", StringType(), True),
                                 StructField("IN_DOWNGRADE_PLANO", StringType(), True),
                                 StructField("NO_TIPO_MULTA_INSTANCIA", StringType(), True),
                                 StructField("VL_MULTA_CHEIA_INSTANCIA", StringType(), True),
                                 StructField("NO_NIVEL_MULTA_INSTANCIA", StringType(), True),
                                 StructField("NO_TIPO_MULTA_CONTA", StringType(), True),
                                 StructField("VL_MULTA_CHEIA_CONTA", StringType(), True),
                                 StructField("NO_NIVEL_MULTA_CONTA", StringType(), True),
                                 StructField("IN_ISENCAO_MULTA", StringType(), True),
                                 StructField("NO_TIPO_ISENCAO", StringType(), True),
                                 StructField("DT_ISENCAO", StringType(), True),
                                 StructField("NO_MOTIVO_ISENCAO", StringType(), True),
                                 StructField("NO_LOGIN_ISENCAO", StringType(), True),
                                 StructField("DT_DESCONEXAO_MULTA_INSTANCIA", StringType(), True),
                                 StructField("DT_DESCONEXAO_MULTA_CONTA", StringType(), True),
                                 StructField("NO_TIPO_FIDELIZACAO_INSTANCIA", StringType(), True),
                                 StructField("NO_TIPO_FIDELIZACAO_CONTA", StringType(), True),
                                 StructField("NO_PRAZO_CONTRATO_MULTA_INST", StringType(), True),
                                 StructField("NO_PRAZO_CONTRATO_MULTA_CONTA", StringType(), True),
                                 StructField("DT_ATIVACAO_MULTA_INSTANCIA", StringType(), True),
                                 StructField("DT_ATIVACAO_MULTA_CONTA", StringType(), True),
                                 StructField("IN_ADM_CONTA_CORPORATIVA", StringType(), True),
                                 StructField("CD_REL_SUBSCRICAO_SIST_ORIG", StringType(), True),
                                 StructField("CD_PLANO_MINUTO", StringType(), True),
                                 StructField("CD_PLANO_MINUTO_LDN", StringType(), True),
                                 StructField("CD_PLANO_MINUTO_VCN", StringType(), True),
                                 StructField("CD_PLANO_MINUTO_VC1", StringType(), True),
                                 StructField("CD_AGRUPADOR_FATURAMENTO", StringType(), True),
                                 StructField("IN_CAMPANHA_OFERTA", StringType(), True),
                                 StructField("NU_EXECUCAO", StringType(), True),
                                 StructField("IN_PRODUTO_WLL", StringType(), True),
                                 StructField("CD_LINHA_TELEFONE", StringType(), True),
                                 StructField("CD_LOCALIDADE_PONTA_B", StringType(), True),
                                 StructField("CD_LOC_NACIONAL_PTA_A", StringType(), True),
                                 StructField("CD_LOC_NACIONAL_PTA_B", StringType(), True),
                                 StructField("SG_LOC_PONTA_A", StringType(), True),
                                 StructField("SG_LOC_PONTA_B", StringType(), True),
                                 StructField("CD_CNL_PONTA_A", StringType(), True),
                                 StructField("CD_CNL_PONTA_B", StringType(), True),
                                 StructField("CD_SUBSCRICAO_ORIG", StringType(), True),
                                 StructField("NO_SITUACAO_PRODUTO", StringType(), True),
                                 StructField("NO_PACOTE_PADRAO_INICIAL", StringType(), True),
                                 StructField("NO_PACOTE_PADRAO_VIGENTE", StringType(), True),
                                 StructField("NO_PACOTE_PADRAO_EXPIRACAO", StringType(), True),
                                 StructField("DS_PERIODO_PERMANENCIA_SAFRA", StringType(), True),
                                 StructField("DS_FRANQUIA_DADOS", StringType(), True),
                                 StructField("NO_BILHETADOR", StringType(), True),
                                 StructField("IN_BLOQUEIO_PID", StringType(), True),
                                 StructField("CD_STATUS_PID", StringType(), True),
                                 StructField("DT_SOLIC_PID", StringType(), True),
                                 StructField("CD_CANAL_PID", StringType(), True),
                                 StructField("IN_PENDENCIA_CADASTRAL_PARCIAL", StringType(), True),
                                 StructField("IN_PENDENCIA_CADASTRAL_TOTAL", StringType(), True),
                                 StructField("IN_FRANQUIA_DADOS_VIGENTE", StringType(), True),
                                 StructField("NU_FAIXA_RAMAL_INICIAL", StringType(), True),
                                 StructField("NU_FAIXA_RAMAL_FINAL", StringType(), True),
                                 StructField("QT_FEIXES_TERMINAIS", StringType(), True),
                                 StructField("IN_TRONCO_CHAVE", StringType(), True),
                                 StructField("IN_TARIFACAO_RAMAL", StringType(), True),
                                 StructField("DS_SEGMENTO_MERCADO", StringType(), True),
                                 StructField("CD_SIMCARD", StringType(), True),
                                 StructField("CD_SERIE_ICCID", StringType(), True),
                                 StructField("NO_EMPRESA_TELECOM", StringType(), True),
                                 StructField("CD_SPE", StringType(), True)
                                ])
    return schema          

# COMMAND ----------

tel_df = spark.read\
               .option("header",True)\
               .schema(getTelSchema())\
               .csv("/mnt/victormrlucasformula1dl/raw/Accenture/MARS_CC67_TELEFONE1_TMP.csv")
tel_df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/accenture/CC67_tel1_tmp.parquet")

DM0_df = spark.read\
               .option("header",True)\
               .schema(getDMSubscricaoSchema())\
               .csv("/mnt/victormrlucasformula1dl/raw/Accenture/DM_00.csv")
DM0_df.write.mode('overwrite').parquet("/mnt/victormrlucasformula1dl/accenture/dm_subscricao.parquet")



# COMMAND ----------

files = dbutils.fs.ls('/mnt/victormrlucasformula1dl/raw/Accenture')

for fi in files: 
  arquivo = fi.name
  df = spark.read\
               .schema(getDMSubscricaoSchema()).csv("/mnt/victormrlucasformula1dl/raw/Accenture/"+arquivo)
  df.write.mode('append').parquet("/mnt/victormrlucasformula1dl/accenture/dm_subscricao.parquet")
  dbutils.fs.mv("/mnt/victormrlucasformula1dl/raw/Accenture/"+arquivo, "/mnt/victormrlucasformula1dl/processed/Accenture/"+arquivo)
  print(arquivo,' loaded!!!!')  
# Databricks notebook source
# MAGIC %pip install pymongo[srv]
# MAGIC %pip install google.cloud.storage==1.44.0
# MAGIC %pip install oauth2client

# COMMAND ----------

import pymongo
from datetime import datetime
import numpy as np
from pymongo import MongoClient
import pandas as pd
from pandas import DataFrame 
from bson.objectid import ObjectId

login = dbutils.secrets.get(scope="formula1-scope",key="mongo-login")
pwd = dbutils.secrets.get(scope="formula1-scope",key="mongo-pass")
server = dbutils.secrets.get(scope="formula1-scope",key="mongo-server")
#returns the Collection from the Home DB MongoDB Atlas
def getHupeCollection():
    uri = "mongodb+srv://"+login+":"+pwd+"@"+server+"/?retryWrites=true&w=majority"
    client = MongoClient(uri)
    #db HOME CollectionSERIES
    db = client.get_database("Pacientes-SESRJ")
    
    return db.get_collection("HUPE") #Collection

# COMMAND ----------

from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials

def getGCPClient():
    client = storage.Client.from_service_account_json("/dbfs/mnt/victormrlucasformula1dl/app/hardy-magpie-203501-79da7bd7faae.json")
    return client 


def listFiles():
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"
    list = []
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = getGCPClient().list_blobs("hupeapp")
    for blob in blobs:
        list.append({
           "name": blob.name
        })
    return list


def uploadGCStorage(pdf):
    bucket_name = "hupeapp"  
    #path = '/' + bucket_name + '/' + str(secure_filename(file.filename))
    bucket = getGCPClient().bucket(bucket_name)
    blob = bucket.blob(pdf)
    blob.content_type = 'application/pdf'
    blob.upload_from_filename("/dbfs/mnt/victormrlucasformula1dl/hupe-raw/"+pdf)              
    
    dbutils.fs.mv("/mnt/victormrlucasformula1dl/hupe-raw/"+pdf, "/mnt/victormrlucasformula1dl/hupe-processed/"+pdf)
    return "pdf loaded"

# COMMAND ----------

def loadAtendToMongo(df):
    for index, row in df.iterrows():
        nome = row['Nome']
        print("Verificando "+nome)
        cpf = row['CPF']
        data = row['Data_Atendimento']
        print(data)
        arrayOfStrings = data.split("/")
        dia = arrayOfStrings[0]
        mes = arrayOfStrings[1]
        ano = arrayOfStrings[2]    
        medicamento = row['Medicamento']
        Processo_Judicial = row['Processo_Judicial']
        origem = row['Origem']
        indicacao = row['INDICACAO']
        agend =  row['AGENDAMENTO']
        obs =  row['OBS']
        laudo =  row['Laudo']

        query = { "CPF": cpf }
        update = { "$setOnInsert": {   
                          "Nome":nome,
                          "CPF": cpf, 
                          "Processo_Judicial": Processo_Judicial, 
                          "Medicamento": medicamento,
                          "Origem": origem,                          
                      },
                      "$addToSet" : 
                          { "Atendimento_Inicial": 
                                {
                                   "Medicamento" : medicamento,
                                   "Agendado" : agend,
                                   "Indicacao" : indicacao,
                                   "Data_Atendimento" : datetime.strptime(ano+"-"+mes+"-"+dia,'%Y-%m-%d'),
                                   "OBS" : obs,
                                   "Laudo" : laudo
                                } 
                         }
                     }
        getHupeCollection().update_one(query, update, upsert=True)
        print(nome,"inserted")

# COMMAND ----------

def loadAplicToMongo(df):    
    df = df.replace(np.nan, ' ')
    df['Origem'] = "Central de Mandados"
    df.loc[df['INDICACAO'].str.contains('OD'), 'INDICACAO'] = 'SIM OD'
    df.loc[df['INDICACAO'].str.contains('OE'), 'INDICACAO'] = 'SIM OE'
    df.loc[df['INDICACAO'].str.contains('AO'), 'INDICACAO'] = 'SIM AO'
    df = df.replace('Não Indicado', 'SEM INDICAÇÃO')
    df = df.replace('Não indicado', 'SEM INDICAÇÃO')
    df = df.replace('Não Compareceu', 'NÃO COMPARECEU')
    df.loc[df['Medicamento'].str.contains('BEVACIZUMABE'), 'Medicamento'] = 'BEVACIZUMABE (Avastin®)'
    df.loc[df['Medicamento'].str.contains('AFLIBERCEPTE'), 'Medicamento'] = 'AFLIBERCEPTE (Eylia®)'
    df.loc[df['Medicamento'].str.contains('RANIBIZUMABE'), 'Medicamento'] = 'RANIBIZUMABE (Lucentis®)'
    
    print(df.to_string())
    
    for index, row in df.iterrows():
        nome = row['Nome']
        print("Verificando "+nome)
        cpf = row['CPF']
        data = row['AGENDAMENTO']
        print(data)
        arrayOfStrings = data.split("/")
        dia = arrayOfStrings[0]
        mes = arrayOfStrings[1]
        ano = arrayOfStrings[2]    
        medicamento = row['Medicamento']
        Processo_Judicial = row['Processo_Judicial']
        origem = row['Origem']
        indicacao = row['INDICACAO']
        agend =  row['AGENDAMENTO']
                
        query = { "CPF": cpf }
        update = { "$setOnInsert": {   
                          "Nome":nome,
                          "CPF":cpf, 
                          "Processo_Judicial": Processo_Judicial, 
                          "Medicamento": medicamento,
                          "Origem" : origem,
                          "Atendimento_Inicial": [
                                {
                                               "Medicamento" : medicamento,
                                               "Agendado" : agend,
                                               "Indicacao" : indicacao,
                                               "Data_Atendimento" : datetime.strptime(ano+"-"+mes+"-"+dia,'%Y-%m-%d'),
                                               "OBS" : " ",
                                               "Laudo" : " "
                                } ]
                      },
                      "$addToSet" : 
                          { 
                            "Agendamento_Aplicacao": 
                                {
                                               "Medicamento" : medicamento,
                                               "Indicacao" : indicacao,
                                               "Data" : datetime.strptime(ano+"-"+mes+"-"+dia,'%Y-%m-%d'),
                                               "OBS" : " "                                              
                                } 
                         }
                     }
        getHupeCollection().update_one(query, update, upsert=True)

# COMMAND ----------

#files = dbutils.fs.ls('/mnt/victormrlucasformula1dl/hupe-raw')

#global pdf
#for fi in files: 
#    arquivo = fi.name
#    if arquivo.find("pdf") > 0:
#       pdf = arquivo
#       uploadGCStorage(pdf)
#       print(pdf, ' loaded')

#dbutils.fs.mv("/mnt/victormrlucasformula1dl/hupe-raw/"+pdf, "/mnt/victormrlucasformula1dl/hupe-processed/"+pdf) 
pdf = 'Laudos20221212.pdf'           
files = dbutils.fs.ls('/mnt/victormrlucasformula1dl/cmrj/hupe/raw') 
for fi in files: 
    arquivo = fi.name
    print(arquivo)
    df = pd.read_csv('/dbfs/mnt/victormrlucasformula1dl/cmrj/hupe/raw/'+arquivo)
    df = df.replace(np.nan, ' ')
    df['Origem'] = "Central de Mandados"
    df['Laudo'] = pdf
    df.loc[df['INDICACAO'].str.contains('SIM OD'), 'INDICACAO'] = 'SIM OD'
    df.loc[df['INDICACAO'].str.contains('SIM OE'), 'INDICACAO'] = 'SIM OE'
    df.loc[df['INDICACAO'].str.contains('SIM AO'), 'INDICACAO'] = 'SIM AO'
    df = df.replace('Não Indicado', 'SEM INDICAÇÃO')
    df = df.replace('Não indicado', 'SEM INDICAÇÃO')
    df = df.replace('Não Compareceu', 'NÃO COMPARECEU')
    df = df.replace('Não compareceu', 'NÃO COMPARECEU')    
    df = df.replace('BEVACIZUMABE', 'BEVACIZUMABE (Avastin®)')
    df = df.replace('AFLIBERCEPTE', 'AFLIBERCEPTE (Eylia®)')
    df = df.replace('RANIBIZUMABE', 'RANIBIZUMABE (Lucentis®)')
    df = df.replace('INTRAVÍTREA', 'INJEÇÃO INTRAVÍTREA')
    df = df.replace('Intravitrea', 'INJEÇÃO INTRAVÍTREA')

    print(df.to_string())
    loadAtendToMongo(df)
    dbutils.fs.mv("/mnt/victormrlucasformula1dl/cmrj/hupe/raw/"+arquivo, "/mnt/victormrlucasformula1dl/cmrj/hupe/processed/"+arquivo) 

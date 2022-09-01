# Databricks notebook source
dbutils.secrets.list("formula1-scope")

# COMMAND ----------

storage_account_name = "victormrlucasformula1dl"
client_id = dbutils.secrets.get(scope="formula1-scope",key="databricks-app-clientid")
tenant_id = dbutils.secrets.get(scope="formula1-scope",key="databricks-app-tenantid")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="databricks-app-clientsecret")

# COMMAND ----------

configs = { "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", 
            "fs.azure.account.oauth2.client.id": f"{client_id}", 
            "fs.azure.account.oauth2.client.secret": f"{client_secret}",
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
     source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
     mount_point = f"/mnt/{storage_account_name}/{container_name}",
     extra_configs = configs)

# COMMAND ----------

#dbutils.fs.ls("/mnt/victormrlucasformula1dl/raw")
dbutils.fs.mounts()
#mount_adls("processed")
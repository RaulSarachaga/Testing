# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Accediendo a un Data Lake <img src="https://bi64pro.com/wp-content/uploads/2019/09/BI64PRO-Logo.png" width="100" height="100"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Montado
# MAGIC 
# MAGIC En el siguiente script realizamos el montado de nuestro Data Lake hacia Azure Databricks

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "d898ad4b-043d-4dda-a046-dab30d317037",
           "fs.azure.account.oauth2.client.secret" : "I4p7:P34/_xDhlzeQNQHBzh:0x@.4Q7b",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e7bcc36a-338a-4c34-9418-60f44d2712d6/oauth2/token"}
dbutils.fs.mount(
  source = "abfss://landing@storage2bi64pro000.dfs.core.windows.net/",
  mount_point = "/mnt/landing",
  extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "d898ad4b-043d-4dda-a046-dab30d317037",
           "fs.azure.account.oauth2.client.secret" : "I4p7:P34/_xDhlzeQNQHBzh:0x@.4Q7b",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e7bcc36a-338a-4c34-9418-60f44d2712d6/oauth2/token"}
dbutils.fs.mount(
  source = "abfss://staging@storage2bi64pro000.dfs.core.windows.net/",
  mount_point = "/mnt/staging",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Desmontado
# MAGIC 
# MAGIC Con el siguiente ejemplo se muestra como realizar un desmontado

# COMMAND ----------

dbutils.fs.unmount("/mnt/landing")
dbutils.fs.unmount("/mnt/staging")

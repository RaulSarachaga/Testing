# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Accediendo a un Data Lake <img src="https://bi64pro.com/wp-content/uploads/2019/09/BI64PRO-Logo.png" width="100" height="100"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Desmontado
# MAGIC 
# MAGIC Con el siguiente ejemplo se muestra como realizar un desmontado

# COMMAND ----------

dbutils.fs.unmount("/mnt/landing")
dbutils.fs.unmount("/mnt/staging")

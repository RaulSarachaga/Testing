# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Accediendo a un Data Lake <img src="https://bi64pro.com/wp-content/uploads/2019/09/BI64PRO-Logo.png" width="100" height="100"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Creación de Tablas
# MAGIC 
# MAGIC En el siguiente ejemplo se muestra como crear una tabla en Azure Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Sucursal
# MAGIC (
# MAGIC Cod_Sucursal INT,Cod_Sucursal_PK INT,Sucursal string ,Latitud string,Longitud string
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (header 'true', inferSchema 'true', delimiter ',')
# MAGIC LOCATION "/mnt/landing/adventuresales/sales_system/dbAventureWorks/Sucursal/Sucursal.csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Sucursal

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE DimSucursal
# MAGIC AS
# MAGIC SELECT T.Cod_Sucursal,T.Sucursal,T.Latitud,T.Longitud
# MAGIC FROM Sucursal T
# MAGIC WHERE Cod_Sucursal<>11

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DimSucursal

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Guardado en el Data Lake
# MAGIC 
# MAGIC En el siguiente ejemplo se muestra como guardar un resultado al Data Lake Store

# COMMAND ----------

df = spark.table('DimSucursal')
df.coalesce(1).write.format("com.databricks.spark.csv").mode ("overwrite").option("header", "true").save("/mnt/staging/adventure_sales/sales_system/dbAdventureWorks/DimSucursal")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Borrado de Tablas
# MAGIC 
# MAGIC En el siguiente ejemplo se muestra como borrar las tablas en un Azure Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE Sucursal;
# MAGIC DROP TABLE DimSucursal;

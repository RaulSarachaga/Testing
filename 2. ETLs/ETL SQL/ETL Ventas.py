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
# MAGIC CREATE TABLE Ventas
# MAGIC (
# MAGIC Cod_Producto STRING,
# MAGIC Cod_Cliente STRING,
# MAGIC Cod_Sucursal STRING,
# MAGIC NumeroOrden STRING,
# MAGIC Cantidad STRING,
# MAGIC PrecioUnitario STRING,
# MAGIC CostoUnitario STRING,
# MAGIC Impuesto STRING,
# MAGIC Flete STRING,
# MAGIC FechaOrden STRING,
# MAGIC FechaEnvio STRING,
# MAGIC FechaVencimiento STRING,
# MAGIC Cod_Promocion STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (header 'true', inferSchema 'true', delimiter ',')
# MAGIC LOCATION "/mnt/landing/adventuresales/sales_system/dbAventureWorks/Ventas/VentasInternet.csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Ventas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE FactVentas
# MAGIC AS
# MAGIC SELECT T.Cod_Sucursal,T.Cod_Producto,T.Cantidad,T.FechaOrdeN
# MAGIC FROM Ventas T

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM FactVentas

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Guardado en el Data Lake
# MAGIC 
# MAGIC En el siguiente ejemplo se muestra como guardar un resultado al Data Lake Store

# COMMAND ----------

df = spark.table('FactVentas')
df.coalesce(1).write.format("com.databricks.spark.csv").mode ("overwrite").option("header", "true").save("/mnt/staging/adventure_sales/sales_system/dbAdventureWorks/FactVentas")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Borrado de Tablas
# MAGIC 
# MAGIC En el siguiente ejemplo se muestra como borrar las tablas en un Azure Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE Ventas;
# MAGIC DROP TABLE FactVentas;

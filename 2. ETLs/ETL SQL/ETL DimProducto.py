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
# MAGIC CREATE TABLE Categoria
# MAGIC (
# MAGIC Cod_Categoria STRING,Nombre STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (header 'true', inferSchema 'true', delimiter ',')
# MAGIC LOCATION "/mnt/landing/adventuresales/sales_system/dbAventureWorks/Categoria/Categoria.csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Categoria

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE SubCategoria
# MAGIC (
# MAGIC Cod_SubCategoria STRING ,Nombre STRING ,Cod_Categoria STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (header 'true', inferSchema 'true', delimiter ',')
# MAGIC LOCATION "/mnt/landing/adventuresales/sales_system/dbAventureWorks/Subcategoria/SubCategoria.csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM SubCategoria

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Producto
# MAGIC (
# MAGIC Cod_Producto STRING,Nombre STRING,Cod_SubCategoria STRING, Color STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (header 'true', inferSchema 'true', delimiter '|')
# MAGIC LOCATION "/mnt/landing/adventuresales/sales_system/dbAventureWorks/Producto/Producto.csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Producto

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE DimProducto
# MAGIC AS
# MAGIC SELECT P.Cod_Producto,P.Nombre as Producto,S.Nombre as Subcategoria, C.Nombre as Categoria 
# MAGIC FROM Producto P
# MAGIC JOIN Subcategoria S ON P.Cod_SubCategoria=S.Cod_SubCategoria
# MAGIC JOIN Categoria C ON C.Cod_Categoria=S.Cod_Categoria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DimProducto

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Guardado en el Data Lake
# MAGIC 
# MAGIC En el siguiente ejemplo se muestra como guardar un resultado al Data Lake Store

# COMMAND ----------

df = spark.table('DimProducto')
df.coalesce(1).write.format("com.databricks.spark.csv").mode ("overwrite").option("header", "true").save("/mnt/staging/adventure_sales/sales_system/dbAdventureWorks/DimProducto")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Borrado de Tablas
# MAGIC 
# MAGIC En el siguiente ejemplo se muestra como borrar las tablas en un Azure Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE Categoria;
# MAGIC DROP TABLE SubCategoria;
# MAGIC DROP TABLE Producto;
# MAGIC DROP TABLE DimProducto;

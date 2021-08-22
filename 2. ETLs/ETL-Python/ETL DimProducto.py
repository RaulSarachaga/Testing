# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # ETLs con Python sobre Azure Databricks <img src="https://bi64pro.com/wp-content/uploads/2019/09/BI64PRO-Logo.png" width="100" height="100"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Leer Datos
# MAGIC 
# MAGIC En este script se muestra como estra datos ubicado en nuestro Data Lake (paso previo se debe de haber montado a Databricks )

# COMMAND ----------

categoriaDF =spark.read.format('csv').options(header='true', inferSchema='true' , delimiter=',').load('/mnt/landing/adventuresales/sales_system/dbAventureWorks/Categoria/Categoria.csv')
display(categoriaDF)

# COMMAND ----------

subcategoriaDF =spark.read.format('csv').options(header='true', inferSchema='true' , delimiter=',').load('/mnt/landing/adventuresales/sales_system/dbAventureWorks/Subcategoria/SubCategoria.csv')
display(subcategoriaDF)

# COMMAND ----------

productoDF =spark.read.format('csv').options(header='true', inferSchema='true' , delimiter='|').load('/mnt/landing/adventuresales/sales_system/dbAventureWorks/Producto/Producto.csv')
display(productoDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Consultas utilizando un Dataframe
# MAGIC 
# MAGIC En este ejemplo se muestra como realizar de Group by sobre un Dataframe en Python

# COMMAND ----------

dimproducto_DF = productoDF.join(subcategoriaDF, subcategoriaDF.Cod_SubCategoria==productoDF.Cod_SubCategoria )

# COMMAND ----------

display(dimproducto_DF)

# COMMAND ----------

dimproductoSelect_DF = dimproducto_DF.select("Cod_Producto")
display(dimproductoSelect_DF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Escribir en un Data Lake
# MAGIC 
# MAGIC En este ejemplo se muestra como escribir un Dataframe sobre un Azure Data Lake Gen2

# COMMAND ----------

dimproductoSelect_DF.coalesce(1).write.format("com.databricks.spark.csv").mode ("overwrite").option("header", "true").save("/mnt/staging/adventure_sales/sales_system/dbAdventureWorks/DimProducto")

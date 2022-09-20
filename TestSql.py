# Databricks notebook source
# MAGIC %run ./Azure_Database_Connection

# COMMAND ----------

empl = "(SELECT TOP (1000) * FROM [dbo].[Employee]) emp_alias"
df_empl = spark.read.jdbc(url=jdbcUrl, table=empl, properties=connectionProperties)
df_empl.createOrReplaceTempView("source") 



# COMMAND ----------

dataS = "(SELECT TOP (1000) * FROM [dbo].[Data]) dataS"
df_dataS = spark.read.jdbc(url=jdbcUrl, table=dataS, properties=connectionProperties)
df_dataS.createOrReplaceTempView("dataS") 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM dataS;

# COMMAND ----------

# DBTITLE 1,Import data to Azure SQL Database 
# df_IImportTable = spark.sql('''
#                                SELECT * From source
#                                Where LastName='Kostov'
# ''')

# df_IImportTable.write \
# .format("jdbc")\
# .option("url", jdbcUrl)\
# .option("dbtable", "KostovImports")\
# .option("user", jdbcUsername)\
# .option("password", jdbcPassword)\
# .save()

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

#%sql
#CREATE or REplace table delta_db.MyDeltaTable (
#                                          FirstName	STRING,
#                                          LastName	STRING,
#                                          Date	TIMESTAMP,
#                                          Amount INT,
#                                          Idate	TIMESTAMP
#)
#USING DELTA
#LOCATION '/FileStore/delta_db/MyDeltaTable'

# COMMAND ----------

#df_res = spark.sql('''
#                    SELECT 
#                    MAX(VERSION)-2 as Version
#                    FROM
#                    (DESCRIBE HISTORY delta_db.MyDeltaTable)
#''').collect()[0][0]

# COMMAND ----------

#df_getData = spark.sql(" SELECT * FROM delta_db.MyDeltaTable VERSION AS OF {}".format(df_res))
#display(df_getData)

# COMMAND ----------

# DBTITLE 1,Import data to Azure SQL Database 
#df_IImportTable = spark.sql('''
#                                with myCTE as (
#                                SELECT * From source
#                                Where LastName='Kostov'
#                                )
#                                SELECT * FROM myCTE
# ''')
#
#df_IImportTable.write \
#.format("jdbc")\
#.option("url", jdbcUrl)\
#.option("dbtable", "KostovImports")\
#.option("user", jdbcUsername)\
#.option("password", jdbcPassword)\
#.save()

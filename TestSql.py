# Databricks notebook source
from pyspark.sql import *
import pandas as pd

# COMMAND ----------

jdbcHostname = "mypersonaldatabaseserver.database.windows.net"
jdbcDatabase = "Dictionary"
jdbcPort = 1433
jdbcUsername = "Kastoraky1"
jdbcPassword = "B.k.12#$56"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

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

# MAGIC %sql
# MAGIC SELECT * FROM fpa.Test2

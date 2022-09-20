# Databricks notebook source
from pyspark.sql import *
import pandas as pd

# COMMAND ----------

jdbcHostname = "mypersonaldatabaseserver.database.windows.net"
jdbcDatabase = "Dictionary"
jdbcPort = 1433
jdbcUsername = "Kastoraky"
jdbcPassword = "B.k.12#$56"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

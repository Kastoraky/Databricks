# Databricks notebook source
# MAGIC %sql
# MAGIC UPDATE fpa.test2
# MAGIC SET Amount = '500'
# MAGIC WHERE LastName = 'KaraIDIOT' and Date='2022-09-21T10:09:38.766+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO fpa.test2 (FirstName, LastName, Date, Amount,Idate)
# MAGIC VALUES ('Izabel','KaraIDIOT',Current_Timestamp(),550,Current_Timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From delta_db.sourceemployee
# MAGIC Where LastName='KaraIDIOT'

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe History delta_db.sourceemployee

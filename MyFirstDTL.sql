-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE SourceEmployee
AS SELECT * FROM fpa.test2

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_Employee
AS SELECT DISTINCT LastName FROM LIVE.SourceEmployee

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Fact_Employee
AS 
SELECT 
LastName,
SUM(Amount) as Amount
FROM LIVE.SourceEmployee
Group by LastName

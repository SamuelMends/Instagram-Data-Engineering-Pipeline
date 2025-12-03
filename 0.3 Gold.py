# Databricks notebook source
# MAGIC %md
# MAGIC ## General Info
# MAGIC | Info | Details |
# MAGIC |-----------|--------|
# MAGIC | Table Name | gold.instagram_gold|
# MAGIC | Source | silver.instagram_silver|
# MAGIC
# MAGIC ## Latest Updates
# MAGIC | Date | Develop by | Reason |
# MAGIC |------|------------|--------|
# MAGIC | 02/12/25 | Samuel Mendes | Notebook Creation |

# COMMAND ----------

#Libraries
from pyspark.sql.functions import current_date, current_timestamp, expr

# COMMAND ----------

#Declaring the schema name of the Table in the catalog
database = 'gold'
table = 'instagram_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   instagram_data.silver.instagram_silver;